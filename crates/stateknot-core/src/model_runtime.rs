// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Runtime-neutral callable model boundary contracts.

use std::{
    error::Error as StdError,
    fmt,
    future::{Future, pending},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_core::Stream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AttemptId, BudgetRemaining, CapabilityIdentity, Failure, ModelCapabilities, ModelDescriptor,
    ModelEvent, ModelProviderModelId, ModelProviderRequestId, ModelProviderResponseId,
    ModelRequest, ModelResponse, ModelResponseMode, ModelUsage, RunId, TenantId, ThreadId,
    Timestamp, TokenCount,
};

/// A heap-allocated, `Send` future whose implementation is independent of an async executor.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A heap-allocated, `Send` stream whose implementation is independent of an async executor.
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// Runtime-provided observation of a permanent cooperative cancellation request.
///
/// Implementations must make [`Self::is_cancelled`] non-blocking and monotonic:
/// once it returns `true`, it must never return `false`. Every future returned
/// by [`Self::cancelled`] must resolve after cancellation without losing a wake
/// that races with future creation. Dropping a wait future must not cancel or
/// consume the shared signal.
pub trait CancellationObserver: Send + Sync + 'static {
    /// Returns whether cancellation has already been requested.
    fn is_cancelled(&self) -> bool;

    /// Returns a cancellation-safe future that resolves once cancellation is requested.
    fn cancelled(&self) -> BoxFuture<'_, ()>;
}

/// Cloneable, runtime-neutral handle to cooperative cancellation state.
///
/// Cancellation is best effort. Observing this signal never proves that a
/// provider did not receive, process, or bill a request. The runtime remains
/// responsible for recording the actual outcome and explicit retry advice.
#[derive(Clone)]
pub struct CancellationSignal {
    observer: Arc<dyn CancellationObserver>,
}

impl CancellationSignal {
    /// Wraps one runtime-specific observer.
    #[must_use]
    pub fn new<O>(observer: O) -> Self
    where
        O: CancellationObserver,
    {
        Self {
            observer: Arc::new(observer),
        }
    }

    /// Wraps an observer that is already shared by the runtime.
    #[must_use]
    pub fn from_shared(observer: Arc<dyn CancellationObserver>) -> Self {
        Self { observer }
    }

    /// Returns a signal that never requests cancellation.
    ///
    /// Production runtimes normally supply a real observer. This value is
    /// useful for deterministic tests and bounded operations whose enclosing
    /// lifetime cannot be cancelled independently.
    #[must_use]
    pub fn never() -> Self {
        Self::new(NeverCancelled)
    }

    /// Returns whether cancellation has already been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.observer.is_cancelled()
    }

    /// Waits asynchronously for cancellation without selecting an executor.
    pub fn cancelled(&self) -> BoxFuture<'_, ()> {
        self.observer.cancelled()
    }
}

impl Default for CancellationSignal {
    fn default() -> Self {
        Self::never()
    }
}

impl fmt::Debug for CancellationSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationSignal")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

struct NeverCancelled;

impl CancellationObserver for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }

    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(pending())
    }
}

/// Reason a model boundary must stop before producing another result.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ModelStopReason {
    /// The enclosing run requested cooperative cancellation.
    Cancelled,
    /// The finite monotonic invocation deadline was reached.
    DeadlineExceeded,
}

/// Ephemeral, capability-limited context for exactly one model attempt.
///
/// The context intentionally is not serializable. It carries stable execution
/// identity, a finite remaining run-budget view, a wall-clock deadline for
/// durable evidence, the equivalent monotonic deadline for timeout enforcement,
/// and a cooperative cancellation signal. It contains no prompt, tool argument,
/// bearer token, credential value, provider SDK client, or mutable property bag.
///
/// Model adapters must treat cancellation and deadline enforcement as separate
/// from provider outcome certainty. Cancellation wins when both conditions are
/// observed together, but neither condition proves that in-flight provider work
/// did not complete or incur usage.
#[derive(Clone)]
pub struct ModelContext {
    tenant_id: TenantId,
    run_id: RunId,
    thread_id: ThreadId,
    attempt_id: AttemptId,
    budget: BudgetRemaining,
    observed_at: Timestamp,
    deadline_instant: Instant,
    cancellation: CancellationSignal,
}

impl ModelContext {
    /// Constructs an ephemeral attempt context from one paired wall/monotonic observation.
    ///
    /// `observed_at` and `observed_instant` must represent the same runtime
    /// observation. The constructor converts the durable budget deadline into
    /// a monotonic deadline so wall-clock adjustments cannot lengthen an active
    /// provider call.
    ///
    /// # Errors
    ///
    /// Returns [`ModelContextError`] when the durable deadline is already
    /// reached or the platform cannot represent the corresponding monotonic
    /// deadline.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        thread_id: ThreadId,
        attempt_id: AttemptId,
        budget: BudgetRemaining,
        observed_at: Timestamp,
        observed_instant: Instant,
        cancellation: CancellationSignal,
    ) -> Result<Self, ModelContextError> {
        let deadline = budget.deadline();
        let remaining_micros = i128::from(deadline.unix_micros())
            .checked_sub(i128::from(observed_at.unix_micros()))
            .expect("subtracting two i64 timestamps cannot overflow i128");
        if remaining_micros <= 0 {
            return Err(ModelContextError::DeadlineReached {
                deadline,
                observed_at,
            });
        }
        let remaining_micros = u64::try_from(remaining_micros)
            .expect("the supported timestamp range fits into u64 microseconds");
        let remaining = Duration::from_micros(remaining_micros);
        let deadline_instant = observed_instant
            .checked_add(remaining)
            .ok_or(ModelContextError::MonotonicDeadlineOutOfRange { remaining })?;

        Ok(Self {
            tenant_id,
            run_id,
            thread_id,
            attempt_id,
            budget,
            observed_at,
            deadline_instant,
            cancellation,
        })
    }

    /// Returns the tenant boundary for storage, policy, and audit correlation.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the durable enclosing run identifier.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the durable conversation thread identifier.
    #[must_use]
    pub const fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    /// Returns the exact attempt identifier required on every response and event.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
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

    /// Returns the durable absolute wall-clock deadline.
    #[must_use]
    pub const fn deadline(&self) -> Timestamp {
        self.budget.deadline()
    }

    /// Returns the equivalent process-local monotonic deadline.
    #[must_use]
    pub const fn deadline_instant(&self) -> Instant {
        self.deadline_instant
    }

    /// Returns the shared cooperative cancellation signal.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    /// Returns remaining monotonic time, or `None` at and after the deadline.
    #[must_use]
    pub fn remaining_time_at(&self, observed_instant: Instant) -> Option<Duration> {
        self.deadline_instant
            .checked_duration_since(observed_instant)
            .filter(|remaining| !remaining.is_zero())
    }

    /// Returns the deterministic stop reason observed at one monotonic instant.
    ///
    /// Explicit cancellation wins if cancellation and deadline are both
    /// observable. This precedence matches the run state-machine contract for
    /// work that has not committed a result.
    #[must_use]
    pub fn stop_reason_at(&self, observed_instant: Instant) -> Option<ModelStopReason> {
        if self.cancellation.is_cancelled() {
            Some(ModelStopReason::Cancelled)
        } else if observed_instant >= self.deadline_instant {
            Some(ModelStopReason::DeadlineExceeded)
        } else {
            None
        }
    }
}

impl fmt::Debug for ModelContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelContext")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("thread_id", &self.thread_id)
            .field("attempt_id", &self.attempt_id)
            .field("observed_at", &self.observed_at)
            .field("deadline", &self.deadline())
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Failure to construct a finite model-attempt context.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelContextError {
    /// The wall-clock observation reached or passed the durable deadline.
    #[error("model context deadline {deadline} was reached at {observed_at}")]
    DeadlineReached {
        /// Durable absolute deadline.
        deadline: Timestamp,
        /// Wall-clock observation used to construct the context.
        observed_at: Timestamp,
    },
    /// The platform monotonic clock could not represent the finite deadline.
    #[error("model context monotonic deadline is out of range after {remaining:?}")]
    MonotonicDeadlineOutOfRange {
        /// Positive duration between the wall-clock observation and deadline.
        remaining: Duration,
    },
}

/// Stage at which one model attempt failed.
///
/// The stage is observability evidence, not retry advice and not proof of
/// provider billing or side-effect outcome. Recovery uses the enclosed
/// [`crate::RetryAdvice`] and the runtime's independent budget, attempt,
/// deadline, idempotency, and policy checks.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ModelErrorPhase {
    /// Local preparation failed before starting a provider exchange.
    Preparation,
    /// A provider exchange was attempted without accepting a complete response or stream.
    Dispatch,
    /// A complete-response exchange failed during provider response handling.
    Response,
    /// An accepted provider stream failed, possibly after semantic events were emitted.
    Stream,
}

/// Correlation evidence attached to a failed model attempt.
///
/// Provider identifiers are opaque diagnostic values. They do not grant
/// authority and are never replay, deduplication, or idempotency keys.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelErrorProvenance {
    attempt_id: AttemptId,
    model: CapabilityIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_model_id: Option<ModelProviderModelId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_request_id: Option<ModelProviderRequestId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_response_id: Option<ModelProviderResponseId>,
}

impl ModelErrorProvenance {
    /// Constructs failure provenance from validated identity and correlation values.
    #[must_use]
    pub fn new(
        attempt_id: AttemptId,
        model: CapabilityIdentity,
        provider_model_id: Option<ModelProviderModelId>,
        provider_request_id: Option<ModelProviderRequestId>,
        provider_response_id: Option<ModelProviderResponseId>,
    ) -> Self {
        Self {
            attempt_id,
            model,
            provider_model_id,
            provider_request_id,
            provider_response_id,
        }
    }

    /// Returns the exact execution-attempt identifier.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the stable owner-qualified model binding identity.
    #[must_use]
    pub const fn model(&self) -> &CapabilityIdentity {
        &self.model
    }

    /// Returns the optional opaque provider model identifier.
    #[must_use]
    pub const fn provider_model_id(&self) -> Option<&ModelProviderModelId> {
        self.provider_model_id.as_ref()
    }

    /// Returns the optional opaque provider request identifier.
    #[must_use]
    pub const fn provider_request_id(&self) -> Option<&ModelProviderRequestId> {
        self.provider_request_id.as_ref()
    }

    /// Returns the optional opaque provider response identifier.
    #[must_use]
    pub const fn provider_response_id(&self) -> Option<&ModelProviderResponseId> {
        self.provider_response_id.as_ref()
    }
}

impl fmt::Debug for ModelErrorProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelErrorProvenance")
            .field("attempt_id", &self.attempt_id)
            .field("model", &self.model)
            .field("provider_model_id", &self.provider_model_id)
            .field("provider_request_id", &self.provider_request_id)
            .field("provider_response_id", &self.provider_response_id)
            .finish_non_exhaustive()
    }
}

/// Public-safe typed failure returned by a model adapter.
///
/// Optional usage is the last complete normalized cumulative snapshot known
/// for the failed attempt. Missing usage means unknown and must never be
/// interpreted as zero. Stream consumers discard partial output as a completed
/// response and separately account for any reported usage.
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelError {
    failure: Failure,
    phase: ModelErrorPhase,
    provenance: ModelErrorProvenance,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<ModelUsage>,
}

impl ModelError {
    /// Constructs a model failure from validated public-safe components.
    #[must_use]
    pub const fn new(
        failure: Failure,
        phase: ModelErrorPhase,
        provenance: ModelErrorProvenance,
        usage: Option<ModelUsage>,
    ) -> Self {
        Self {
            failure,
            phase,
            provenance,
            usage,
        }
    }

    /// Revalidates attempt, model, response-mode, and usage bindings.
    ///
    /// # Errors
    ///
    /// Returns [`ModelErrorValidationError`] when this failure does not belong
    /// to the exact context/descriptor/request snapshot or its reported usage
    /// exceeds the request ceilings.
    pub fn validate_for(
        &self,
        context: &ModelContext,
        descriptor: &ModelDescriptor,
        request: &ModelRequest,
    ) -> Result<(), ModelErrorValidationError> {
        self.validate_for_attempt(context.attempt_id, descriptor, request)
    }

    /// Revalidates this failure against a durable attempt identity and exact
    /// descriptor/request snapshot.
    ///
    /// This is the persistence-safe counterpart to [`Self::validate_for`]: a
    /// recovered ledger has no process-local monotonic clock or cancellation
    /// handle, but it must still reject attempt, model, delivery-mode, and usage
    /// substitution before using the failure as retry evidence.
    ///
    /// # Errors
    ///
    /// Returns [`ModelErrorValidationError`] for any binding mismatch.
    pub fn validate_for_attempt(
        &self,
        attempt_id: AttemptId,
        descriptor: &ModelDescriptor,
        request: &ModelRequest,
    ) -> Result<(), ModelErrorValidationError> {
        if self.provenance.attempt_id != attempt_id {
            return Err(ModelErrorValidationError::AttemptMismatch {
                expected: attempt_id,
                actual: self.provenance.attempt_id,
            });
        }

        let expected_model = descriptor.metadata().identity();
        if &self.provenance.model != expected_model {
            return Err(ModelErrorValidationError::ModelIdentityMismatch {
                expected: Box::new(expected_model.clone()),
                actual: Box::new(self.provenance.model.clone()),
            });
        }

        let phase_matches_mode = match self.phase {
            ModelErrorPhase::Preparation | ModelErrorPhase::Dispatch => true,
            ModelErrorPhase::Response => request.response_mode() == ModelResponseMode::Complete,
            ModelErrorPhase::Stream => request.response_mode() == ModelResponseMode::Streaming,
        };
        if !phase_matches_mode {
            return Err(ModelErrorValidationError::PhaseResponseModeMismatch {
                phase: self.phase,
                response_mode: request.response_mode(),
            });
        }

        if let Some(usage) = &self.usage {
            if usage.input_tokens() > request.limits().max_input_tokens() {
                return Err(ModelErrorValidationError::InputUsageExceedsRequest {
                    maximum: request.limits().max_input_tokens(),
                    actual: usage.input_tokens(),
                });
            }
            if usage.output_tokens() > request.limits().max_output_tokens() {
                return Err(ModelErrorValidationError::OutputUsageExceedsRequest {
                    maximum: request.limits().max_output_tokens(),
                    actual: usage.output_tokens(),
                });
            }
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

    /// Returns the stage at which the attempt failed.
    #[must_use]
    pub const fn phase(&self) -> ModelErrorPhase {
        self.phase
    }

    /// Returns attempt and provider correlation evidence.
    #[must_use]
    pub const fn provenance(&self) -> &ModelErrorProvenance {
        &self.provenance
    }

    /// Returns the last complete normalized usage snapshot, when known.
    #[must_use]
    pub const fn usage(&self) -> Option<&ModelUsage> {
        self.usage.as_ref()
    }
}

impl fmt::Debug for ModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelError")
            .field("failure", &self.failure)
            .field("phase", &self.phase)
            .field("provenance", &self.provenance)
            .field("usage", &self.usage)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, formatter)
    }
}

impl StdError for ModelError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.failure)
    }
}

/// Invalid relationship between a model failure and its invocation snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelErrorValidationError {
    /// Failure provenance named a different attempt.
    #[error("model failure attempt {actual} does not match context attempt {expected}")]
    AttemptMismatch {
        /// Expected context attempt.
        expected: AttemptId,
        /// Rejected failure attempt.
        actual: AttemptId,
    },
    /// Failure provenance named a different registered model binding.
    #[error("model failure identity {actual:?} does not match descriptor {expected:?}")]
    ModelIdentityMismatch {
        /// Exact immutable descriptor identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected failure claim.
        actual: Box<CapabilityIdentity>,
    },
    /// Failure stage was impossible for the request's delivery mode.
    #[error("model failure phase {phase:?} is invalid for {response_mode:?} delivery")]
    PhaseResponseModeMismatch {
        /// Reported failure stage.
        phase: ModelErrorPhase,
        /// Immutable request delivery mode.
        response_mode: ModelResponseMode,
    },
    /// Provider input accounting exceeded the request ceiling.
    #[error("failed model attempt input usage {actual} exceeds request maximum {maximum}")]
    InputUsageExceedsRequest {
        /// Request input-token ceiling.
        maximum: TokenCount,
        /// Provider-reported normalized input.
        actual: TokenCount,
    },
    /// Provider output accounting exceeded the request ceiling.
    #[error("failed model attempt output usage {actual} exceeds request maximum {maximum}")]
    OutputUsageExceedsRequest {
        /// Request output-token ceiling.
        maximum: TokenCount,
        /// Provider-reported normalized output.
        actual: TokenCount,
    },
}

/// Object-safe, provider-neutral model execution boundary.
///
/// Implementations must validate the immutable descriptor capabilities,
/// registered extensions, and referenced schemas before provider I/O. Unary
/// invocation accepts only [`ModelResponseMode::Complete`]; streaming accepts
/// only [`ModelResponseMode::Streaming`]. Every response/event/error must use
/// the context attempt and descriptor identity.
///
/// An adapter must not hide provider SDK retries beneath one [`AttemptId`]. It
/// disables such retries or arranges for the runtime to create and account for
/// a distinct attempt before every provider exchange. It enforces the monotonic
/// deadline and cooperative signal while preserving outcome uncertainty.
/// A stream emits at most one terminal `Err`, emits nothing afterward, and
/// never converts transport EOF, provider failure, cancellation, or malformed
/// output into a successful terminal event.
pub trait Model: Send + Sync + 'static {
    /// Returns the immutable descriptor for this exact registered binding.
    fn descriptor(&self) -> &ModelDescriptor;

    /// Returns the capabilities frozen into the descriptor snapshot.
    fn capabilities(&self) -> &ModelCapabilities {
        self.descriptor().capabilities()
    }

    /// Executes one complete-response model attempt.
    fn invoke(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'_, Result<ModelResponse, ModelError>>;

    /// Executes one streaming model attempt.
    fn stream(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxStream<'_, Result<ModelEvent, ModelError>>;
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use serde_json::{Value, json, to_value};

    use super::*;
    use crate::{
        BudgetUsage, DurationMillis, FailureCategory, FailureCode, FailureId, FailureMessage,
        FailureOrigin, ResolvedBudget, RetryAdvice,
    };

    const ATTEMPT_ID: &str = "01912345-6789-7abc-8def-0123456789ab";
    const RUN_ID: &str = "01912345-6789-7abc-8def-0123456789ac";
    const THREAD_ID: &str = "01912345-6789-7abc-8def-0123456789ad";
    const FAILURE_ID: &str = "01912345-6789-7abc-8def-0123456789ae";
    const OBSERVED_AT: &str = "2029-12-31T23:59:59.000000Z";

    struct AlwaysCancelled;

    impl CancellationObserver for AlwaysCancelled {
        fn is_cancelled(&self) -> bool {
            true
        }

        fn cancelled(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    fn descriptor() -> ModelDescriptor {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/core-model-descriptor-v1.json"
        ))
        .unwrap();
        serde_json::from_value(fixture["descriptors"]["valid"][0].clone()).unwrap()
    }

    fn request(response_mode: ModelResponseMode) -> ModelRequest {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-model-request-v1.json"))
                .unwrap();
        let mut value = fixture["requests"]["valid"][0].clone();
        if response_mode == ModelResponseMode::Streaming {
            value["response_mode"] = Value::from("streaming");
            value["requirements"]["streaming"] = Value::Bool(true);
        }
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

    fn context_with(cancellation: CancellationSignal) -> ModelContext {
        let observed_at = OBSERVED_AT.parse::<Timestamp>().unwrap();
        ModelContext::new(
            TenantId::new("tenant-production").unwrap(),
            RUN_ID.parse().unwrap(),
            THREAD_ID.parse().unwrap(),
            ATTEMPT_ID.parse().unwrap(),
            remaining_budget(observed_at),
            observed_at,
            Instant::now(),
            cancellation,
        )
        .unwrap()
    }

    fn failure() -> Failure {
        Failure::new(
            FAILURE_ID.parse::<FailureId>().unwrap(),
            FailureCategory::RateLimited,
            FailureCode::new("provider.rate_limited").unwrap(),
            FailureOrigin::new("provider.example").unwrap(),
            FailureMessage::new("The model provider is temporarily unavailable.").unwrap(),
            RetryAdvice::SafeAfter {
                delay: DurationMillis::new(250).unwrap(),
            },
        )
        .unwrap()
    }

    fn provenance(descriptor: &ModelDescriptor, attempt_id: AttemptId) -> ModelErrorProvenance {
        ModelErrorProvenance::new(
            attempt_id,
            descriptor.metadata().identity().clone(),
            Some(ModelProviderModelId::new("provider-model-v1").unwrap()),
            Some(ModelProviderRequestId::new("req_01JABCDEF").unwrap()),
            Some(ModelProviderResponseId::new("resp_01JABCDEF").unwrap()),
        )
    }

    #[test]
    fn cancellation_signal_is_runtime_neutral_cloneable_and_redacted() {
        let never = CancellationSignal::never();
        assert!(!never.is_cancelled());
        assert_eq!(
            format!("{never:?}"),
            "CancellationSignal { cancelled: false, .. }"
        );

        let cancelled = CancellationSignal::new(AlwaysCancelled);
        assert!(cancelled.is_cancelled());
        assert!(cancelled.clone().is_cancelled());
        assert_eq!(
            format!("{cancelled:?}"),
            "CancellationSignal { cancelled: true, .. }"
        );
    }

    #[test]
    fn model_context_derives_a_finite_monotonic_deadline() {
        let observed_at = OBSERVED_AT.parse::<Timestamp>().unwrap();
        let observed_instant = Instant::now();
        let context = ModelContext::new(
            TenantId::new("tenant-production").unwrap(),
            RUN_ID.parse().unwrap(),
            THREAD_ID.parse().unwrap(),
            ATTEMPT_ID.parse().unwrap(),
            remaining_budget(observed_at),
            observed_at,
            observed_instant,
            CancellationSignal::never(),
        )
        .unwrap();

        let expected_remaining = Duration::from_secs(1);
        assert_eq!(context.observed_at(), observed_at);
        assert_eq!(
            context.remaining_time_at(observed_instant),
            Some(expected_remaining)
        );
        assert_eq!(
            context.deadline_instant(),
            observed_instant + expected_remaining
        );
        assert_eq!(context.stop_reason_at(observed_instant), None);
        assert_eq!(
            context.stop_reason_at(context.deadline_instant()),
            Some(ModelStopReason::DeadlineExceeded)
        );
        assert_eq!(context.remaining_time_at(context.deadline_instant()), None);
    }

    #[test]
    fn model_context_rejects_reached_deadlines_and_cancellation_wins() {
        let observed_at = "2030-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
        let error = ModelContext::new(
            TenantId::new("tenant-production").unwrap(),
            RUN_ID.parse().unwrap(),
            THREAD_ID.parse().unwrap(),
            ATTEMPT_ID.parse().unwrap(),
            remaining_budget(OBSERVED_AT.parse().unwrap()),
            observed_at,
            Instant::now(),
            CancellationSignal::never(),
        )
        .unwrap_err();
        assert!(matches!(error, ModelContextError::DeadlineReached { .. }));

        let context = context_with(CancellationSignal::new(AlwaysCancelled));
        assert_eq!(
            context.stop_reason_at(context.deadline_instant()),
            Some(ModelStopReason::Cancelled)
        );
    }

    #[test]
    fn model_context_debug_omits_budget_values_and_monotonic_clock() {
        let context = context_with(CancellationSignal::never());
        let debug = format!("{context:?}");
        assert!(debug.contains("tenant-production"));
        assert!(debug.contains(ATTEMPT_ID));
        assert!(!debug.contains("1000000"));
        assert!(!debug.contains("Instant"));
    }

    #[test]
    fn model_error_round_trips_public_evidence_without_leaking_identifiers() {
        let descriptor = descriptor();
        let error = ModelError::new(
            failure(),
            ModelErrorPhase::Stream,
            provenance(&descriptor, ATTEMPT_ID.parse().unwrap()),
            Some(ModelUsage::new(TokenCount::new(120), None, TokenCount::new(7), None).unwrap()),
        );
        let value = to_value(&error).unwrap();
        assert_eq!(value["phase"], "stream");
        assert_eq!(value["provenance"]["provider_request_id"], "req_01JABCDEF");
        let decoded = serde_json::from_value::<ModelError>(value.clone()).unwrap();
        assert_eq!(to_value(decoded).unwrap(), value);
        assert_eq!(
            error.to_string(),
            "The model provider is temporarily unavailable."
        );

        let debug = format!("{error:?}");
        assert!(!debug.contains("req_01JABCDEF"));
        assert!(!debug.contains("resp_01JABCDEF"));
        assert!(!debug.contains("provider-model-v1"));
        assert!(!debug.contains("temporarily unavailable"));
    }

    #[test]
    fn model_error_validation_binds_attempt_mode_model_and_usage() {
        let descriptor = descriptor();
        let context = context_with(CancellationSignal::never());
        let complete = request(ModelResponseMode::Complete);
        let valid = ModelError::new(
            failure(),
            ModelErrorPhase::Response,
            provenance(&descriptor, context.attempt_id()),
            Some(ModelUsage::new(TokenCount::new(120), None, TokenCount::new(7), None).unwrap()),
        );
        valid
            .validate_for(&context, &descriptor, &complete)
            .unwrap();

        let wrong_attempt = ModelError::new(
            failure(),
            ModelErrorPhase::Response,
            provenance(&descriptor, RUN_ID.parse::<AttemptId>().unwrap()),
            None,
        );
        assert!(matches!(
            wrong_attempt.validate_for(&context, &descriptor, &complete),
            Err(ModelErrorValidationError::AttemptMismatch { .. })
        ));

        let wrong_phase = ModelError::new(
            failure(),
            ModelErrorPhase::Stream,
            provenance(&descriptor, context.attempt_id()),
            None,
        );
        assert_eq!(
            wrong_phase.validate_for(&context, &descriptor, &complete),
            Err(ModelErrorValidationError::PhaseResponseModeMismatch {
                phase: ModelErrorPhase::Stream,
                response_mode: ModelResponseMode::Complete,
            })
        );

        let excessive = ModelError::new(
            failure(),
            ModelErrorPhase::Response,
            provenance(&descriptor, context.attempt_id()),
            Some(
                ModelUsage::new(
                    TokenCount::new(complete.limits().max_input_tokens().get() + 1),
                    None,
                    TokenCount::ZERO,
                    None,
                )
                .unwrap(),
            ),
        );
        assert!(matches!(
            excessive.validate_for(&context, &descriptor, &complete),
            Err(ModelErrorValidationError::InputUsageExceedsRequest { .. })
        ));
    }

    #[test]
    fn response_provenance_preserves_provider_request_correlation() {
        let descriptor = descriptor();
        let provenance = crate::ModelResponseProvenance::new(
            ATTEMPT_ID.parse().unwrap(),
            descriptor.metadata().identity().clone(),
            None,
            None,
        )
        .with_provider_request_id(ModelProviderRequestId::new("req_01JABCDEF").unwrap());
        assert_eq!(
            provenance.provider_request_id().unwrap().as_str(),
            "req_01JABCDEF"
        );
        assert_eq!(
            to_value(&provenance).unwrap()["provider_request_id"],
            "req_01JABCDEF"
        );
        assert!(!format!("{provenance:?}").contains("req_01JABCDEF"));
    }

    struct EmptyStream;

    impl Stream for EmptyStream {
        type Item = Result<ModelEvent, ModelError>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }

    struct FakeModel {
        descriptor: ModelDescriptor,
    }

    impl Model for FakeModel {
        fn descriptor(&self) -> &ModelDescriptor {
            &self.descriptor
        }

        fn invoke(
            &self,
            _: ModelContext,
            _: ModelRequest,
        ) -> BoxFuture<'_, Result<ModelResponse, ModelError>> {
            Box::pin(pending())
        }

        fn stream(
            &self,
            _: ModelContext,
            _: ModelRequest,
        ) -> BoxStream<'_, Result<ModelEvent, ModelError>> {
            Box::pin(EmptyStream)
        }
    }

    #[test]
    fn model_trait_is_object_safe_and_boundary_values_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn accept_object(_: &dyn Model) {}

        assert_send_sync::<ModelContext>();
        assert_send_sync::<ModelError>();
        assert_send_sync::<CancellationSignal>();

        let model = FakeModel {
            descriptor: descriptor(),
        };
        accept_object(&model);
        assert_eq!(model.capabilities(), model.descriptor().capabilities());
    }

    #[test]
    fn model_error_schemas_are_closed_and_wire_rejects_unknown_fields() {
        for schema in [
            to_value(schemars::schema_for!(ModelErrorProvenance)).unwrap(),
            to_value(schemars::schema_for!(ModelError)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }

        let descriptor = descriptor();
        let error = ModelError::new(
            failure(),
            ModelErrorPhase::Dispatch,
            provenance(&descriptor, ATTEMPT_ID.parse().unwrap()),
            None,
        );
        let mut value = to_value(error).unwrap();
        value["provider_payload"] = json!({ "secret": true });
        assert!(serde_json::from_value::<ModelError>(value).is_err());
    }
}
