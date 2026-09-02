// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable-before-dispatch model and tool attempt execution.

use std::{
    error::Error as StdError,
    fmt,
    future::poll_fn,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use stateknot_core::{
    AgentResultProvenance, AttemptId, BoundedJson, BoxFuture, BudgetRemaining, CancellationSignal,
    Digest, EventId, ExecutionCount, Failure, FailureCategory, FailureCode, FailureId,
    FailureMessage, FailureOrigin, InvocationId, JournalAppend, JournalEventIntent,
    JournalEventKind, JournalExpectation, JournalPayload, ModelContext, ModelContextError,
    ModelError, ModelErrorPhase, ModelErrorProvenance, ModelEvent, ModelEventAccumulator,
    ModelInvocation, ModelInvocationStatus, ModelInvocationTransition, ModelRequest, ModelResponse,
    ModelResponseMode, ModelStopReason, RetryAdvice, RunFence, SchemaReference, Timestamp,
    ToolContext, ToolContextError, ToolError, ToolErrorPhase, ToolErrorProvenance,
    ToolExternalEffect, ToolInputValidationError, ToolInvocation, ToolInvocationStatus,
    ToolInvocationTransition, ToolProgressSink, ToolResult, ToolRisk, ToolStopReason,
};
use stateknot_store_postgres::{
    ModelInvocationCommitOutcome, PostgresStore, StoreError, ToolInvocationCommitOutcome,
};
use thiserror::Error;

use crate::{
    JsonSchemaRegistry, ModelProviderRegistry, ModelProviderRegistryError,
    StandardInvocationExecutionSchemaError, ToolProviderRegistry, ToolProviderRegistryError,
    standard_invocation_execution_event_schema,
};

const MAX_MUTATION_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Trusted wall/monotonic clock pair captured at one process observation.
#[derive(Clone, Copy, Debug)]
pub struct InvocationClockObservation {
    observed_at: Timestamp,
    observed_instant: Instant,
}

impl InvocationClockObservation {
    /// Constructs one trusted paired observation.
    #[must_use]
    pub const fn new(observed_at: Timestamp, observed_instant: Instant) -> Self {
        Self {
            observed_at,
            observed_instant,
        }
    }

    /// Returns the wall-clock component used for durable budget decisions.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns the paired monotonic observation used for active deadlines.
    #[must_use]
    pub const fn observed_instant(self) -> Instant {
        self.observed_instant
    }
}

/// Trusted clock source for attempt contexts.
pub trait InvocationClock: Send + Sync + 'static {
    /// Captures paired wall and monotonic observations.
    fn observe(&self) -> Result<InvocationClockObservation, InvocationClockError>;
}

/// Production system/monotonic clock implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemInvocationClock;

impl InvocationClock for SystemInvocationClock {
    fn observe(&self) -> Result<InvocationClockObservation, InvocationClockError> {
        let observed_instant = Instant::now();
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| InvocationClockError::BeforeUnixEpoch)?;
        let micros = i64::try_from(elapsed.as_micros())
            .map_err(|_| InvocationClockError::TimestampOutOfRange)?;
        let observed_at = Timestamp::from_unix_micros(micros)
            .map_err(|_| InvocationClockError::TimestampOutOfRange)?;
        Ok(InvocationClockObservation::new(
            observed_at,
            observed_instant,
        ))
    }
}

/// Failure to capture a representable runtime clock observation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum InvocationClockError {
    /// Host wall time preceded the Unix epoch.
    #[error("invocation system clock precedes the Unix epoch")]
    BeforeUnixEpoch,
    /// Host wall time exceeded the canonical timestamp range.
    #[error("invocation system clock is outside the supported timestamp range")]
    TimestampOutOfRange,
}

/// Closed external boundary whose next-attempt budget is requested.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum InvocationBoundaryKind {
    /// One model provider attempt.
    Model,
    /// One tool implementation attempt.
    Tool,
}

/// Payload-free trusted request to an admission/accounting implementation.
#[derive(Clone, Debug)]
pub struct InvocationBudgetContext {
    provenance: AgentResultProvenance,
    boundary: InvocationBoundaryKind,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    intent_digest: Digest,
    observed_at: Timestamp,
}

impl InvocationBudgetContext {
    /// Returns immutable admitted run provenance loaded from storage.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the external boundary being admitted.
    #[must_use]
    pub const fn boundary(&self) -> InvocationBoundaryKind {
        self.boundary
    }

    /// Returns the stable logical invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the proposed physical attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the exact durable invocation-intent checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the trusted clock used to evaluate remaining capacity.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
}

/// Trusted admission/accounting source for exact remaining run capacity.
///
/// Implementations load the immutable resolved budget and cumulative durable
/// usage by `provenance`, recheck the intent/attempt admission policy, and
/// return a finite [`BudgetRemaining`] evaluated at `observed_at`. The executor
/// never accepts a caller-authored remaining-budget value directly.
pub trait InvocationBudgetProvider: Send + Sync + 'static {
    /// Resolves exact remaining capacity before a durable attempt start.
    fn remaining(
        &self,
        context: InvocationBudgetContext,
    ) -> BoxFuture<'_, Result<BudgetRemaining, InvocationBudgetProviderError>>;
}

type PrivateBudgetSource = dyn StdError + Send + Sync + 'static;

/// Payload-redacted trusted accounting-provider failure.
#[derive(Clone)]
pub struct InvocationBudgetProviderError {
    private_source: Arc<PrivateBudgetSource>,
}

impl InvocationBudgetProviderError {
    /// Wraps one private admission/accounting diagnostic.
    #[must_use]
    pub fn new<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            private_source: Arc::new(source),
        }
    }

    /// Returns the private source to trusted in-process diagnostics only.
    #[must_use]
    pub fn private_source(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.private_source.as_ref()
    }
}

impl fmt::Debug for InvocationBudgetProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvocationBudgetProviderError")
            .field("has_private_source", &true)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for InvocationBudgetProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invocation budget provider failed")
    }
}

impl StdError for InvocationBudgetProviderError {}

/// Runtime-owned sink for already validated semantic model stream events.
///
/// Implementations must durably deduplicate by `(attempt_id, sequence)` before
/// exposing events externally. A successful future asserts ordered acceptance;
/// the final accumulated response remains authoritative only after its separate
/// invocation-ledger commit.
pub trait ModelEventSink: Send + Sync + 'static {
    /// Accepts one identity-bound, contiguous semantic event.
    fn emit(&self, event: ModelEvent) -> BoxFuture<'_, Result<(), ModelEventSinkError>>;
}

type PrivateEventSinkSource = dyn StdError + Send + Sync + 'static;

/// Payload-redacted model event sink failure.
#[derive(Clone)]
pub struct ModelEventSinkError {
    private_source: Arc<PrivateEventSinkSource>,
}

impl ModelEventSinkError {
    /// Wraps one private durable-sink diagnostic.
    #[must_use]
    pub fn new<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            private_source: Arc::new(source),
        }
    }

    /// Returns the private source to trusted in-process diagnostics only.
    #[must_use]
    pub fn private_source(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.private_source.as_ref()
    }
}

impl fmt::Debug for ModelEventSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelEventSinkError")
            .field("has_private_source", &true)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ModelEventSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("model event sink failed")
    }
}

impl StdError for ModelEventSinkError {}

/// Stable start and terminal event identities for one physical attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvocationAttemptEventIds {
    start: EventId,
    terminal: EventId,
}

impl InvocationAttemptEventIds {
    /// Binds two distinct preallocated event identities.
    ///
    /// # Errors
    ///
    /// Rejects identity reuse between start and terminal facts.
    pub fn new(start: EventId, terminal: EventId) -> Result<Self, InvocationAttemptEventIdsError> {
        if start == terminal {
            return Err(InvocationAttemptEventIdsError::Duplicate);
        }
        Ok(Self { start, terminal })
    }

    /// Generates two `UUIDv7` identities for a newly retained handoff.
    #[must_use]
    pub fn generate() -> Self {
        Self {
            start: EventId::generate(),
            terminal: EventId::generate(),
        }
    }

    /// Returns the durable-before-dispatch start event identity.
    #[must_use]
    pub const fn start(self) -> EventId {
        self.start
    }

    /// Returns the stable terminal response/error event identity.
    #[must_use]
    pub const fn terminal(self) -> EventId {
        self.terminal
    }
}

/// Invalid attempt event identity pair.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum InvocationAttemptEventIdsError {
    /// Start and terminal facts reused one event identity.
    #[error("invocation start and terminal event identities must be distinct")]
    Duplicate,
}

/// Retained model-attempt handoff safe to retry before physical dispatch.
#[derive(Clone)]
pub struct ModelAttemptHandoff {
    fence: RunFence,
    invocation: ModelInvocation,
    attempt_id: AttemptId,
    events: InvocationAttemptEventIds,
    cancellation: CancellationSignal,
    stream_sink: Option<Arc<dyn ModelEventSink>>,
}

impl ModelAttemptHandoff {
    /// Constructs and scope-checks one model attempt handoff.
    ///
    /// A streaming request requires a durable event sink; a complete request
    /// rejects one so event-delivery configuration cannot be silently ignored.
    ///
    /// # Errors
    ///
    /// Rejects crossed scope, a non-startable invocation state, or an invalid
    /// response-mode/sink pairing.
    pub fn new(
        fence: RunFence,
        invocation: ModelInvocation,
        attempt_id: AttemptId,
        events: InvocationAttemptEventIds,
        cancellation: CancellationSignal,
        stream_sink: Option<Arc<dyn ModelEventSink>>,
    ) -> Result<Self, InvocationAttemptHandoffError> {
        validate_model_handoff(&fence, &invocation, stream_sink.is_some())?;
        Ok(Self {
            fence,
            invocation,
            attempt_id,
            events,
            cancellation,
            stream_sink,
        })
    }

    /// Returns the exact retained worker fence.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the prepared or safely retryable durable invocation revision.
    #[must_use]
    pub const fn invocation(&self) -> &ModelInvocation {
        &self.invocation
    }

    /// Returns the proposed unique physical provider attempt.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns stable start/terminal journal identities.
    #[must_use]
    pub const fn events(&self) -> InvocationAttemptEventIds {
        self.events
    }
}

impl fmt::Debug for ModelAttemptHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelAttemptHandoff")
            .field("fence", &self.fence)
            .field("invocation_head", &self.invocation.head())
            .field("attempt_id", &self.attempt_id)
            .field("events", &self.events)
            .field("streaming", &self.stream_sink.is_some())
            .finish_non_exhaustive()
    }
}

/// Retained tool-attempt handoff safe to retry before physical dispatch.
#[derive(Clone)]
pub struct ToolAttemptHandoff {
    fence: RunFence,
    invocation: ToolInvocation,
    attempt_id: AttemptId,
    events: InvocationAttemptEventIds,
    cancellation: CancellationSignal,
    progress_sink: Option<Arc<dyn ToolProgressSink>>,
}

impl ToolAttemptHandoff {
    /// Constructs and scope-checks one tool attempt handoff.
    ///
    /// # Errors
    ///
    /// Rejects crossed scope or a non-startable durable invocation state.
    pub fn new(
        fence: RunFence,
        invocation: ToolInvocation,
        attempt_id: AttemptId,
        events: InvocationAttemptEventIds,
        cancellation: CancellationSignal,
        progress_sink: Option<Arc<dyn ToolProgressSink>>,
    ) -> Result<Self, InvocationAttemptHandoffError> {
        validate_tool_handoff(&fence, &invocation)?;
        Ok(Self {
            fence,
            invocation,
            attempt_id,
            events,
            cancellation,
            progress_sink,
        })
    }

    /// Returns the exact retained worker fence.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the prepared or safely retryable durable invocation revision.
    #[must_use]
    pub const fn invocation(&self) -> &ToolInvocation {
        &self.invocation
    }

    /// Returns the proposed unique physical tool attempt.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns stable start/terminal journal identities.
    #[must_use]
    pub const fn events(&self) -> InvocationAttemptEventIds {
        self.events
    }
}

impl fmt::Debug for ToolAttemptHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolAttemptHandoff")
            .field("fence", &self.fence)
            .field("invocation_head", &self.invocation.head())
            .field("attempt_id", &self.attempt_id)
            .field("events", &self.events)
            .field("progress", &self.progress_sink.is_some())
            .finish_non_exhaustive()
    }
}

/// Invalid durable attempt handoff.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum InvocationAttemptHandoffError {
    /// Fence and invocation crossed tenant/run scope.
    #[error("invocation attempt handoff crosses its retained worker fence")]
    ScopeMismatch,
    /// Invocation is executing or already terminal.
    #[error("invocation attempt handoff does not reference a startable revision")]
    InvocationNotStartable,
    /// Streaming request omitted its required durable semantic-event sink.
    #[error("streaming model attempt requires a durable event sink")]
    MissingStreamSink,
    /// Complete-response request supplied an unused stream sink.
    #[error("complete model attempt must not supply a stream event sink")]
    UnexpectedStreamSink,
}

fn validate_model_handoff(
    fence: &RunFence,
    invocation: &ModelInvocation,
    has_stream_sink: bool,
) -> Result<(), InvocationAttemptHandoffError> {
    if invocation.intent().tenant_id() != fence.tenant_id()
        || invocation.intent().run_id() != fence.run_id()
        || invocation.journal_head().tenant_id() != fence.tenant_id()
        || invocation.journal_head().run_id() != fence.run_id()
    {
        return Err(InvocationAttemptHandoffError::ScopeMismatch);
    }
    if !matches!(
        invocation.status(),
        ModelInvocationStatus::Prepared | ModelInvocationStatus::Failed
    ) {
        return Err(InvocationAttemptHandoffError::InvocationNotStartable);
    }
    match (
        invocation.intent().request().response_mode(),
        has_stream_sink,
    ) {
        (ModelResponseMode::Streaming, false) => {
            Err(InvocationAttemptHandoffError::MissingStreamSink)
        }
        (ModelResponseMode::Complete, true) => {
            Err(InvocationAttemptHandoffError::UnexpectedStreamSink)
        }
        (ModelResponseMode::Complete, false) | (ModelResponseMode::Streaming, true) => Ok(()),
    }
}

fn validate_tool_handoff(
    fence: &RunFence,
    invocation: &ToolInvocation,
) -> Result<(), InvocationAttemptHandoffError> {
    if invocation.intent().tenant_id() != fence.tenant_id()
        || invocation.intent().run_id() != fence.run_id()
        || invocation.journal_head().tenant_id() != fence.tenant_id()
        || invocation.journal_head().run_id() != fence.run_id()
    {
        return Err(InvocationAttemptHandoffError::ScopeMismatch);
    }
    if !matches!(
        invocation.status(),
        ToolInvocationStatus::Prepared | ToolInvocationStatus::Failed
    ) {
        return Err(InvocationAttemptHandoffError::InvocationNotStartable);
    }
    Ok(())
}

/// Bounded identical-mutation retry policy for invocation ledger commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableInvocationExecutorOptions {
    maximum_mutation_attempts: u8,
    mutation_retry_initial_delay: Duration,
}

impl DurableInvocationExecutorOptions {
    /// Absolute identical transaction-attempt ceiling.
    pub const HARD_MAXIMUM_MUTATION_ATTEMPTS: u8 = 10;

    /// Constructs a bounded lost-acknowledgement retry policy.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive attempts or a zero/greater-than-one-second delay.
    pub fn new(
        maximum_mutation_attempts: u8,
        mutation_retry_initial_delay: Duration,
    ) -> Result<Self, DurableInvocationExecutorOptionsError> {
        if maximum_mutation_attempts == 0
            || maximum_mutation_attempts > Self::HARD_MAXIMUM_MUTATION_ATTEMPTS
        {
            return Err(DurableInvocationExecutorOptionsError::InvalidMutationAttempts);
        }
        if mutation_retry_initial_delay.is_zero()
            || mutation_retry_initial_delay > MAX_MUTATION_RETRY_DELAY
        {
            return Err(DurableInvocationExecutorOptionsError::InvalidMutationRetryDelay);
        }
        Ok(Self {
            maximum_mutation_attempts,
            mutation_retry_initial_delay,
        })
    }

    /// Returns maximum identical database mutation attempts.
    #[must_use]
    pub const fn maximum_mutation_attempts(self) -> u8 {
        self.maximum_mutation_attempts
    }

    /// Returns the first exponential retry delay.
    #[must_use]
    pub const fn mutation_retry_initial_delay(self) -> Duration {
        self.mutation_retry_initial_delay
    }
}

impl Default for DurableInvocationExecutorOptions {
    fn default() -> Self {
        Self {
            maximum_mutation_attempts: 3,
            mutation_retry_initial_delay: Duration::from_millis(25),
        }
    }
}

/// Invalid durable invocation executor retry policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableInvocationExecutorOptionsError {
    /// Mutation attempts were zero or above the hard ceiling.
    #[error("invocation executor mutation attempt count is invalid")]
    InvalidMutationAttempts,
    /// Initial mutation delay was zero or above one second.
    #[error("invocation executor mutation retry delay is invalid")]
    InvalidMutationRetryDelay,
}

/// Whether a newly dispatched model attempt committed a response or error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelAttemptTerminalKind {
    /// Complete unary or accumulated streaming response committed.
    Response,
    /// Adapter/provider/supervisor failure evidence committed.
    Error,
}

/// Closed result of one model attempt executor call.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ModelAttemptOutcome {
    /// This call performed one physical dispatch and committed its terminal fact.
    Dispatched {
        /// Whether response or error evidence won.
        terminal: ModelAttemptTerminalKind,
        /// Exact terminal invocation revision.
        invocation: ModelInvocation,
    },
    /// The start event had already committed, so no provider call was repeated.
    Recovered {
        /// Current exact invocation revision loaded after duplicate suppression.
        invocation: ModelInvocation,
    },
}

/// Whether a newly dispatched tool attempt committed a result or error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolAttemptTerminalKind {
    /// Validated tool result committed.
    Result,
    /// Known or ambiguous failure evidence committed.
    Error,
}

/// Closed result of one tool attempt executor call.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ToolAttemptOutcome {
    /// This call performed one physical dispatch and committed its terminal fact.
    Dispatched {
        /// Whether result or error evidence won.
        terminal: ToolAttemptTerminalKind,
        /// Exact terminal invocation revision.
        invocation: ToolInvocation,
    },
    /// The start event had already committed, so no tool call was repeated.
    Recovered {
        /// Current exact invocation revision loaded after duplicate suppression.
        invocation: ToolInvocation,
    },
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
enum ModelTerminalEvidence {
    Response(ModelResponse),
    Error(ModelError),
}

/// Retained terminal model evidence after a database commit failure.
///
/// Debug output excludes the response/error payload. Pass this value back to
/// [`DurableInvocationExecutor::commit_model_terminal`] without modification;
/// no provider request is performed by that method.
#[derive(Clone)]
pub struct ModelTerminalCommitHandoff {
    fence: RunFence,
    invocation: ModelInvocation,
    event_id: EventId,
    evidence: ModelTerminalEvidence,
}

impl fmt::Debug for ModelTerminalCommitHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelTerminalCommitHandoff")
            .field("fence", &self.fence)
            .field("invocation_head", &self.invocation.head())
            .field("event_id", &self.event_id)
            .field("terminal", &self.kind())
            .finish_non_exhaustive()
    }
}

impl ModelTerminalCommitHandoff {
    /// Returns response/error classification without exposing payload content.
    #[must_use]
    pub const fn kind(&self) -> ModelAttemptTerminalKind {
        match &self.evidence {
            ModelTerminalEvidence::Response(_) => ModelAttemptTerminalKind::Response,
            ModelTerminalEvidence::Error(_) => ModelAttemptTerminalKind::Error,
        }
    }

    /// Returns the worker fence currently attached to terminal evidence.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Rebinds retained evidence to a newer live fence in the same run.
    ///
    /// This is required when a provider call outlives its original lease. The
    /// store remains authoritative: it validates that the replacement fence is
    /// currently live before accepting the already-produced terminal evidence.
    ///
    /// # Errors
    ///
    /// Rejects a fence from another tenant or run.
    pub fn rebind_fence(mut self, fence: RunFence) -> Result<Self, InvocationAttemptHandoffError> {
        if self.invocation.intent().tenant_id() != fence.tenant_id()
            || self.invocation.intent().run_id() != fence.run_id()
        {
            return Err(InvocationAttemptHandoffError::ScopeMismatch);
        }
        self.fence = fence;
        Ok(self)
    }
}

#[derive(Clone)]
enum ToolTerminalEvidence {
    Result(ToolResult),
    Error(ToolError),
}

/// Retained terminal tool evidence after a database commit failure.
///
/// Debug output excludes tool result/error payloads. Retrying this handoff never
/// calls the tool again.
#[derive(Clone)]
pub struct ToolTerminalCommitHandoff {
    fence: RunFence,
    invocation: ToolInvocation,
    event_id: EventId,
    evidence: ToolTerminalEvidence,
}

impl fmt::Debug for ToolTerminalCommitHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolTerminalCommitHandoff")
            .field("fence", &self.fence)
            .field("invocation_head", &self.invocation.head())
            .field("event_id", &self.event_id)
            .field("terminal", &self.kind())
            .finish_non_exhaustive()
    }
}

impl ToolTerminalCommitHandoff {
    /// Returns result/error classification without exposing payload content.
    #[must_use]
    pub const fn kind(&self) -> ToolAttemptTerminalKind {
        match &self.evidence {
            ToolTerminalEvidence::Result(_) => ToolAttemptTerminalKind::Result,
            ToolTerminalEvidence::Error(_) => ToolAttemptTerminalKind::Error,
        }
    }

    /// Returns the worker fence currently attached to terminal evidence.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Rebinds retained evidence to a newer live fence in the same run.
    ///
    /// This changes no result/error bytes and never calls the tool again. The
    /// durable store validates current fence ownership at commit time.
    ///
    /// # Errors
    ///
    /// Rejects a fence from another tenant or run.
    pub fn rebind_fence(mut self, fence: RunFence) -> Result<Self, InvocationAttemptHandoffError> {
        if self.invocation.intent().tenant_id() != fence.tenant_id()
            || self.invocation.intent().run_id() != fence.run_id()
        {
            return Err(InvocationAttemptHandoffError::ScopeMismatch);
        }
        self.fence = fence;
        Ok(self)
    }
}

/// Payload-safe cause of a retained terminal invocation commit failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InvocationTerminalCommitFailure {
    /// The durable store rejected or could not complete the mutation.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Runtime-owned standard event data could not be encoded or validated.
    #[error("terminal invocation event payload is invalid")]
    InvalidEventPayload,
    /// Runtime-owned worker journal metadata violated its invariant.
    #[error("terminal invocation journal append is invalid")]
    InvalidJournalAppend,
    /// The run lost its durable journal anchor before terminal evidence could commit.
    #[error("terminal invocation run journal is unavailable")]
    RunJournalUnavailable,
    /// Retained terminal evidence did not reference an executing attempt.
    #[error("terminal invocation does not reference an executing attempt")]
    InvalidInvocationState,
    /// The store returned a future commit outcome unknown to this runtime.
    #[error("terminal invocation store outcome is unsupported by this runtime")]
    UnsupportedStoreOutcome,
}

/// Terminal model commit failure retaining exact retry evidence.
#[derive(Debug, Error)]
#[error("model terminal invocation commit failed: {source}")]
pub struct ModelTerminalCommitError {
    #[source]
    source: InvocationTerminalCommitFailure,
    recovery: Box<ModelTerminalCommitHandoff>,
}

impl ModelTerminalCommitError {
    /// Returns the payload-redacted store failure.
    #[must_use]
    pub const fn source_error(&self) -> &InvocationTerminalCommitFailure {
        &self.source
    }

    /// Recovers the exact response/error commit handoff for a no-dispatch retry.
    #[must_use]
    pub fn into_recovery(self) -> ModelTerminalCommitHandoff {
        *self.recovery
    }
}

/// Terminal tool commit failure retaining exact retry evidence.
#[derive(Debug, Error)]
#[error("tool terminal invocation commit failed: {source}")]
pub struct ToolTerminalCommitError {
    #[source]
    source: InvocationTerminalCommitFailure,
    recovery: Box<ToolTerminalCommitHandoff>,
}

impl ToolTerminalCommitError {
    /// Returns the payload-redacted store failure.
    #[must_use]
    pub const fn source_error(&self) -> &InvocationTerminalCommitFailure {
        &self.source
    }

    /// Recovers the exact result/error commit handoff for a no-dispatch retry.
    #[must_use]
    pub fn into_recovery(self) -> ToolTerminalCommitHandoff {
        *self.recovery
    }
}

/// First-party durable model/tool attempt executor.
#[derive(Clone)]
pub struct DurableInvocationExecutor {
    store: PostgresStore,
    schemas: JsonSchemaRegistry,
    event_schema: SchemaReference,
    models: ModelProviderRegistry,
    tools: ToolProviderRegistry,
    budget: Arc<dyn InvocationBudgetProvider>,
    clock: Arc<dyn InvocationClock>,
    options: DurableInvocationExecutorOptions,
}

impl DurableInvocationExecutor {
    /// Builds an executor using the production system/monotonic clock.
    ///
    /// # Errors
    ///
    /// Fails unless the exact embedded invocation journal schema is installed
    /// in the frozen offline registry.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        models: ModelProviderRegistry,
        tools: ToolProviderRegistry,
        budget: Arc<dyn InvocationBudgetProvider>,
        options: DurableInvocationExecutorOptions,
    ) -> Result<Self, DurableInvocationExecutorBuildError> {
        Self::with_clock(
            store,
            schemas,
            models,
            tools,
            budget,
            Arc::new(SystemInvocationClock),
            options,
        )
    }

    /// Builds an executor with an injected trusted paired clock.
    ///
    /// # Errors
    ///
    /// Fails unless the exact embedded invocation journal schema is installed.
    #[allow(clippy::too_many_arguments)]
    pub fn with_clock(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        models: ModelProviderRegistry,
        tools: ToolProviderRegistry,
        budget: Arc<dyn InvocationBudgetProvider>,
        clock: Arc<dyn InvocationClock>,
        options: DurableInvocationExecutorOptions,
    ) -> Result<Self, DurableInvocationExecutorBuildError> {
        let (event_schema, _) = standard_invocation_execution_event_schema()?;
        if !schemas.contains(&event_schema) {
            return Err(DurableInvocationExecutorBuildError::MissingEventSchema);
        }
        Ok(Self {
            store,
            schemas,
            event_schema,
            models,
            tools,
            budget,
            clock,
            options,
        })
    }

    async fn prepare_model_context(
        &self,
        handoff: &ModelAttemptHandoff,
    ) -> Result<ModelContext, ModelAttemptExecutionError> {
        let run = self
            .store
            .load_run(handoff.fence.tenant_id(), handoff.fence.run_id())
            .await?;
        let provenance = run.lifecycle().provenance().clone();
        validate_run_provenance(&provenance, &handoff.fence)?;
        let observation = self.clock.observe()?;
        let remaining = self
            .budget
            .remaining(InvocationBudgetContext {
                provenance: provenance.clone(),
                boundary: InvocationBoundaryKind::Model,
                invocation_id: handoff.invocation.intent().invocation_id(),
                attempt_id: handoff.attempt_id,
                intent_digest: handoff.invocation.intent().intent_digest(),
                observed_at: observation.observed_at,
            })
            .await?;
        validate_model_budget(&remaining, handoff.invocation.intent().request())?;
        let context = ModelContext::new(
            provenance.tenant_id().clone(),
            provenance.run_id(),
            provenance.thread_id(),
            handoff.attempt_id,
            remaining,
            observation.observed_at,
            observation.observed_instant,
            handoff.cancellation.clone(),
        )?;
        if let Some(reason) = context.stop_reason_at(Instant::now()) {
            return Err(ModelAttemptExecutionError::StoppedBeforeStart { reason });
        }
        Ok(context)
    }

    async fn prepare_tool_context(
        &self,
        handoff: &ToolAttemptHandoff,
    ) -> Result<ToolContext, ToolAttemptExecutionError> {
        let run = self
            .store
            .load_run(handoff.fence.tenant_id(), handoff.fence.run_id())
            .await?;
        let provenance = run.lifecycle().provenance().clone();
        validate_tool_run_provenance(&provenance, &handoff.fence)?;
        let observation = self.clock.observe()?;
        let remaining = self
            .budget
            .remaining(InvocationBudgetContext {
                provenance: provenance.clone(),
                boundary: InvocationBoundaryKind::Tool,
                invocation_id: handoff.invocation.intent().invocation_id(),
                attempt_id: handoff.attempt_id,
                intent_digest: handoff.invocation.intent().intent_digest(),
                observed_at: observation.observed_at,
            })
            .await?;
        validate_tool_budget(
            &remaining,
            handoff.invocation.intent().descriptor().semantics().risk(),
        )?;
        let context = Self::build_tool_context(handoff, &provenance, remaining, observation)?;
        handoff
            .invocation
            .intent()
            .input()
            .validate_for(&context, handoff.invocation.intent().descriptor())?;
        if let Some(reason) = context.stop_reason_at(Instant::now()) {
            return Err(ToolAttemptExecutionError::StoppedBeforeStart { reason });
        }
        Ok(context)
    }

    fn build_tool_context(
        handoff: &ToolAttemptHandoff,
        provenance: &AgentResultProvenance,
        remaining: BudgetRemaining,
        observation: InvocationClockObservation,
    ) -> Result<ToolContext, ToolContextError> {
        let intent = handoff.invocation.intent();
        if let Some(progress_sink) = &handoff.progress_sink {
            ToolContext::new_with_progress(
                provenance.tenant_id().clone(),
                provenance.run_id(),
                provenance.thread_id(),
                intent.invocation_id(),
                handoff.attempt_id,
                intent.descriptor(),
                remaining,
                intent.effective_limits().timeout(),
                observation.observed_at,
                observation.observed_instant,
                handoff.cancellation.clone(),
                Arc::clone(progress_sink),
            )
        } else {
            ToolContext::new(
                provenance.tenant_id().clone(),
                provenance.run_id(),
                provenance.thread_id(),
                intent.invocation_id(),
                handoff.attempt_id,
                intent.descriptor(),
                remaining,
                intent.effective_limits().timeout(),
                observation.observed_at,
                observation.observed_instant,
                handoff.cancellation.clone(),
            )
        }
    }

    async fn recover_model_if_started(
        &self,
        handoff: &ModelAttemptHandoff,
    ) -> Result<Option<ModelAttemptOutcome>, ModelAttemptExecutionError> {
        let current = self
            .store
            .load_model_invocation(
                handoff.fence.tenant_id(),
                handoff.fence.run_id(),
                handoff.invocation.intent().invocation_id(),
            )
            .await?;
        if current.intent() != handoff.invocation.intent() {
            return Err(ModelAttemptExecutionError::RecoveredInvocationMismatch);
        }
        if current.head() == handoff.invocation.head() {
            return Ok(None);
        }
        if current.attempt_id() != Some(handoff.attempt_id) {
            return Err(ModelAttemptExecutionError::InvocationAdvanced);
        }
        Ok(Some(ModelAttemptOutcome::Recovered {
            invocation: current,
        }))
    }

    async fn recover_tool_if_started(
        &self,
        handoff: &ToolAttemptHandoff,
    ) -> Result<Option<ToolAttemptOutcome>, ToolAttemptExecutionError> {
        let current = self
            .store
            .load_tool_invocation(
                handoff.fence.tenant_id(),
                handoff.fence.run_id(),
                handoff.invocation.intent().invocation_id(),
            )
            .await?;
        if current.intent() != handoff.invocation.intent() {
            return Err(ToolAttemptExecutionError::RecoveredInvocationMismatch);
        }
        if current.head() == handoff.invocation.head() {
            return Ok(None);
        }
        if current.attempt_id() != Some(handoff.attempt_id) {
            return Err(ToolAttemptExecutionError::InvocationAdvanced);
        }
        Ok(Some(ToolAttemptOutcome::Recovered {
            invocation: current,
        }))
    }

    /// Executes at most one physical model provider exchange.
    ///
    /// `StartAttempt` commits before provider I/O. If that event is found
    /// idempotently, this method loads current state and never calls the model.
    /// A terminal database failure returns the exact response/error recovery
    /// handoff so callers can retry persistence without repeating provider I/O.
    pub fn execute_model(
        &self,
        handoff: ModelAttemptHandoff,
    ) -> BoxFuture<'_, Result<ModelAttemptOutcome, ModelAttemptExecutionError>> {
        Box::pin(self.execute_model_inner(handoff))
    }

    async fn execute_model_inner(
        &self,
        handoff: ModelAttemptHandoff,
    ) -> Result<ModelAttemptOutcome, ModelAttemptExecutionError> {
        validate_model_handoff(
            &handoff.fence,
            &handoff.invocation,
            handoff.stream_sink.is_some(),
        )?;
        if let Some(recovered) = self.recover_model_if_started(&handoff).await? {
            return Ok(recovered);
        }
        let provider = self
            .models
            .resolve(handoff.invocation.intent().descriptor())?;
        let context = self.prepare_model_context(&handoff).await?;

        let start_transition = ModelInvocationTransition::StartAttempt {
            attempt_id: handoff.attempt_id,
        };
        let start_payload = self.event_payload(
            "model-attempt-started",
            "model_attempt_started",
            "model",
            handoff.invocation.intent().invocation_id(),
            handoff.attempt_id,
            handoff.invocation.intent().intent_digest(),
        )?;
        let start_append = worker_append(
            &handoff.fence,
            handoff.invocation.journal_head().clone(),
            handoff.events.start,
            start_payload,
        )
        .map_err(|_| ModelAttemptExecutionError::JournalAppend)?;
        let start = self
            .advance_model_with_retry(start_append, &handoff.invocation, start_transition)
            .await?;
        let executing = match start {
            ModelInvocationCommitOutcome::Committed { invocation, .. } => invocation,
            ModelInvocationCommitOutcome::Idempotent { .. } => {
                let current = self
                    .store
                    .load_model_invocation(
                        handoff.fence.tenant_id(),
                        handoff.fence.run_id(),
                        handoff.invocation.intent().invocation_id(),
                    )
                    .await?;
                if current.intent() != handoff.invocation.intent() {
                    return Err(ModelAttemptExecutionError::RecoveredInvocationMismatch);
                }
                if current.attempt_id() != Some(handoff.attempt_id) {
                    return Err(ModelAttemptExecutionError::InvocationAdvanced);
                }
                return Ok(ModelAttemptOutcome::Recovered {
                    invocation: current,
                });
            }
            _ => return Err(ModelAttemptExecutionError::UnsupportedStoreOutcome),
        };

        let evidence = match context.stop_reason_at(Instant::now()) {
            Some(reason) => ModelTerminalEvidence::Error(model_stop_error(
                &context,
                executing.intent().descriptor(),
                reason,
            )),
            None => {
                self.dispatch_model(
                    provider,
                    context,
                    executing.intent().request().clone(),
                    handoff.stream_sink,
                )
                .await
            }
        };
        let terminal = ModelTerminalCommitHandoff {
            fence: handoff.fence,
            invocation: executing,
            event_id: handoff.events.terminal,
            evidence,
        };
        self.commit_model_terminal(terminal)
            .await
            .map_err(ModelAttemptExecutionError::Terminal)
    }

    /// Commits retained terminal model evidence without provider I/O.
    pub fn commit_model_terminal(
        &self,
        handoff: ModelTerminalCommitHandoff,
    ) -> BoxFuture<'_, Result<ModelAttemptOutcome, ModelTerminalCommitError>> {
        Box::pin(self.commit_model_terminal_inner(handoff))
    }

    async fn commit_model_terminal_inner(
        &self,
        handoff: ModelTerminalCommitHandoff,
    ) -> Result<ModelAttemptOutcome, ModelTerminalCommitError> {
        let terminal_kind = handoff.kind();
        let (operation, event_kind, transition) = match &handoff.evidence {
            ModelTerminalEvidence::Response(response) => (
                "model_response_committed",
                "model-response-committed",
                ModelInvocationTransition::RecordResponse {
                    response: response.clone(),
                },
            ),
            ModelTerminalEvidence::Error(error) => (
                "model_error_committed",
                "model-error-committed",
                ModelInvocationTransition::RecordError {
                    error: error.clone(),
                },
            ),
        };
        let Some(attempt_id) = handoff.invocation.attempt_id() else {
            return Err(ModelTerminalCommitError {
                source: InvocationTerminalCommitFailure::InvalidInvocationState,
                recovery: Box::new(handoff),
            });
        };
        let Ok(payload) = self.event_payload(
            event_kind,
            operation,
            "model",
            handoff.invocation.intent().invocation_id(),
            attempt_id,
            handoff.invocation.intent().intent_digest(),
        ) else {
            return Err(ModelTerminalCommitError {
                source: InvocationTerminalCommitFailure::InvalidEventPayload,
                recovery: Box::new(handoff),
            });
        };
        let run = match self
            .store
            .load_run(
                handoff.invocation.intent().tenant_id(),
                handoff.invocation.intent().run_id(),
            )
            .await
        {
            Ok(run) => run,
            Err(source) => {
                return Err(ModelTerminalCommitError {
                    source: InvocationTerminalCommitFailure::Store(source),
                    recovery: Box::new(handoff),
                });
            }
        };
        let Some(journal_head) = run.journal_head().cloned() else {
            return Err(ModelTerminalCommitError {
                source: InvocationTerminalCommitFailure::RunJournalUnavailable,
                recovery: Box::new(handoff),
            });
        };
        let Ok(append) = worker_append(&handoff.fence, journal_head, handoff.event_id, payload)
        else {
            return Err(ModelTerminalCommitError {
                source: InvocationTerminalCommitFailure::InvalidJournalAppend,
                recovery: Box::new(handoff),
            });
        };
        match self
            .advance_model_with_retry(append, &handoff.invocation, transition)
            .await
        {
            Ok(outcome) => Ok(ModelAttemptOutcome::Dispatched {
                terminal: terminal_kind,
                invocation: outcome.invocation().clone(),
            }),
            Err(source) => Err(ModelTerminalCommitError {
                source: InvocationTerminalCommitFailure::Store(source),
                recovery: Box::new(handoff),
            }),
        }
    }

    /// Executes at most one physical tool implementation call.
    ///
    /// Start duplicate suppression and retained terminal recovery have the same
    /// no-repeat semantics as [`Self::execute_model`].
    pub fn execute_tool(
        &self,
        handoff: ToolAttemptHandoff,
    ) -> BoxFuture<'_, Result<ToolAttemptOutcome, ToolAttemptExecutionError>> {
        Box::pin(self.execute_tool_inner(handoff))
    }

    async fn execute_tool_inner(
        &self,
        handoff: ToolAttemptHandoff,
    ) -> Result<ToolAttemptOutcome, ToolAttemptExecutionError> {
        validate_tool_handoff(&handoff.fence, &handoff.invocation)?;
        if let Some(recovered) = self.recover_tool_if_started(&handoff).await? {
            return Ok(recovered);
        }
        let provider = self
            .tools
            .resolve(handoff.invocation.intent().descriptor())?;
        let context = self.prepare_tool_context(&handoff).await?;

        let start_payload = self.event_payload(
            "tool-attempt-started",
            "tool_attempt_started",
            "tool",
            handoff.invocation.intent().invocation_id(),
            handoff.attempt_id,
            handoff.invocation.intent().intent_digest(),
        )?;
        let start_append = worker_append(
            &handoff.fence,
            handoff.invocation.journal_head().clone(),
            handoff.events.start,
            start_payload,
        )
        .map_err(|_| ToolAttemptExecutionError::JournalAppend)?;
        let start = self
            .advance_tool_with_retry(
                start_append,
                &handoff.invocation,
                ToolInvocationTransition::StartAttempt {
                    attempt_id: handoff.attempt_id,
                },
            )
            .await?;
        let executing = match start {
            ToolInvocationCommitOutcome::Committed { invocation, .. } => invocation,
            ToolInvocationCommitOutcome::Idempotent { .. } => {
                let current = self
                    .store
                    .load_tool_invocation(
                        handoff.fence.tenant_id(),
                        handoff.fence.run_id(),
                        handoff.invocation.intent().invocation_id(),
                    )
                    .await?;
                if current.intent() != handoff.invocation.intent() {
                    return Err(ToolAttemptExecutionError::RecoveredInvocationMismatch);
                }
                if current.attempt_id() != Some(handoff.attempt_id) {
                    return Err(ToolAttemptExecutionError::InvocationAdvanced);
                }
                return Ok(ToolAttemptOutcome::Recovered {
                    invocation: current,
                });
            }
            _ => return Err(ToolAttemptExecutionError::UnsupportedStoreOutcome),
        };

        let evidence = if let Some(reason) = context.stop_reason_at(Instant::now()) {
            ToolTerminalEvidence::Error(tool_stop_error(
                &context,
                executing.intent().descriptor(),
                reason,
            ))
        } else {
            let input = executing.intent().input().clone();
            self.dispatch_tool(provider, context, input).await
        };
        let terminal = ToolTerminalCommitHandoff {
            fence: handoff.fence,
            invocation: executing,
            event_id: handoff.events.terminal,
            evidence,
        };
        self.commit_tool_terminal(terminal)
            .await
            .map_err(ToolAttemptExecutionError::Terminal)
    }

    /// Commits retained terminal tool evidence without calling the tool.
    pub fn commit_tool_terminal(
        &self,
        handoff: ToolTerminalCommitHandoff,
    ) -> BoxFuture<'_, Result<ToolAttemptOutcome, ToolTerminalCommitError>> {
        Box::pin(self.commit_tool_terminal_inner(handoff))
    }

    async fn commit_tool_terminal_inner(
        &self,
        handoff: ToolTerminalCommitHandoff,
    ) -> Result<ToolAttemptOutcome, ToolTerminalCommitError> {
        let terminal_kind = handoff.kind();
        let (operation, event_kind, transition) = match &handoff.evidence {
            ToolTerminalEvidence::Result(result) => (
                "tool_result_committed",
                "tool-result-committed",
                ToolInvocationTransition::RecordResult {
                    result: result.clone(),
                },
            ),
            ToolTerminalEvidence::Error(error) => (
                "tool_error_committed",
                "tool-error-committed",
                ToolInvocationTransition::RecordError {
                    error: error.clone(),
                },
            ),
        };
        let Some(attempt_id) = handoff.invocation.attempt_id() else {
            return Err(ToolTerminalCommitError {
                source: InvocationTerminalCommitFailure::InvalidInvocationState,
                recovery: Box::new(handoff),
            });
        };
        let Ok(payload) = self.event_payload(
            event_kind,
            operation,
            "tool",
            handoff.invocation.intent().invocation_id(),
            attempt_id,
            handoff.invocation.intent().intent_digest(),
        ) else {
            return Err(ToolTerminalCommitError {
                source: InvocationTerminalCommitFailure::InvalidEventPayload,
                recovery: Box::new(handoff),
            });
        };
        let run = match self
            .store
            .load_run(
                handoff.invocation.intent().tenant_id(),
                handoff.invocation.intent().run_id(),
            )
            .await
        {
            Ok(run) => run,
            Err(source) => {
                return Err(ToolTerminalCommitError {
                    source: InvocationTerminalCommitFailure::Store(source),
                    recovery: Box::new(handoff),
                });
            }
        };
        let Some(journal_head) = run.journal_head().cloned() else {
            return Err(ToolTerminalCommitError {
                source: InvocationTerminalCommitFailure::RunJournalUnavailable,
                recovery: Box::new(handoff),
            });
        };
        let Ok(append) = worker_append(&handoff.fence, journal_head, handoff.event_id, payload)
        else {
            return Err(ToolTerminalCommitError {
                source: InvocationTerminalCommitFailure::InvalidJournalAppend,
                recovery: Box::new(handoff),
            });
        };
        match self
            .advance_tool_with_retry(append, &handoff.invocation, transition)
            .await
        {
            Ok(outcome) => Ok(ToolAttemptOutcome::Dispatched {
                terminal: terminal_kind,
                invocation: outcome.invocation().clone(),
            }),
            Err(source) => Err(ToolTerminalCommitError {
                source: InvocationTerminalCommitFailure::Store(source),
                recovery: Box::new(handoff),
            }),
        }
    }

    async fn dispatch_model(
        &self,
        provider: Arc<dyn stateknot_core::Model>,
        context: ModelContext,
        request: ModelRequest,
        stream_sink: Option<Arc<dyn ModelEventSink>>,
    ) -> ModelTerminalEvidence {
        match request.response_mode() {
            ModelResponseMode::Complete => {
                Self::dispatch_complete_model(provider, context, request).await
            }
            ModelResponseMode::Streaming => {
                Self::dispatch_streaming_model(provider, context, request, stream_sink).await
            }
        }
    }

    async fn dispatch_complete_model(
        provider: Arc<dyn stateknot_core::Model>,
        context: ModelContext,
        request: ModelRequest,
    ) -> ModelTerminalEvidence {
        let result = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => {
                return ModelTerminalEvidence::Error(model_stop_error(
                    &context,
                    provider.descriptor(),
                    ModelStopReason::Cancelled,
                ));
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(context.deadline_instant())) => {
                return ModelTerminalEvidence::Error(model_stop_error(
                    &context,
                    provider.descriptor(),
                    ModelStopReason::DeadlineExceeded,
                ));
            }
            result = provider.invoke(context.clone(), request.clone()) => result,
        };
        match result {
            Ok(response)
                if response.provenance().attempt_id() == context.attempt_id()
                    && response
                        .validate_for(provider.descriptor(), &request)
                        .is_ok() =>
            {
                ModelTerminalEvidence::Response(response)
            }
            Ok(_) => ModelTerminalEvidence::Error(model_contract_error(
                &context,
                provider.descriptor(),
                ModelErrorPhase::Response,
                "runtime.model.invalid_response",
                "Model provider returned a response outside its registered contract.",
            )),
            Err(error)
                if error
                    .validate_for(&context, provider.descriptor(), &request)
                    .is_ok() =>
            {
                ModelTerminalEvidence::Error(error)
            }
            Err(_) => ModelTerminalEvidence::Error(model_contract_error(
                &context,
                provider.descriptor(),
                ModelErrorPhase::Response,
                "runtime.model.invalid_error",
                "Model provider returned failure evidence outside its registered contract.",
            )),
        }
    }

    async fn dispatch_streaming_model(
        provider: Arc<dyn stateknot_core::Model>,
        context: ModelContext,
        request: ModelRequest,
        stream_sink: Option<Arc<dyn ModelEventSink>>,
    ) -> ModelTerminalEvidence {
        let Some(sink) = stream_sink else {
            return ModelTerminalEvidence::Error(model_contract_error(
                &context,
                provider.descriptor(),
                ModelErrorPhase::Preparation,
                "runtime.model.missing_stream_sink",
                "Streaming model execution has no durable event sink.",
            ));
        };
        let Ok(mut accumulator) =
            ModelEventAccumulator::new(context.attempt_id(), provider.descriptor(), &request)
        else {
            return ModelTerminalEvidence::Error(model_contract_error(
                &context,
                provider.descriptor(),
                ModelErrorPhase::Preparation,
                "runtime.model.invalid_stream_setup",
                "Streaming model execution could not satisfy its registered contract.",
            ));
        };
        let mut stream = provider.stream(context.clone(), request.clone());
        loop {
            let next = tokio::select! {
                biased;
                () = context.cancellation().cancelled() => {
                    return ModelTerminalEvidence::Error(model_stop_error(
                        &context,
                        provider.descriptor(),
                        ModelStopReason::Cancelled,
                    ));
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(context.deadline_instant())) => {
                    return ModelTerminalEvidence::Error(model_stop_error(
                        &context,
                        provider.descriptor(),
                        ModelStopReason::DeadlineExceeded,
                    ));
                }
                next = poll_fn(|task| stream.as_mut().poll_next(task)) => next,
            };
            match next {
                Some(Ok(event)) => {
                    if let Err(evidence) = Self::accept_model_stream_event(
                        provider.as_ref(),
                        &context,
                        &mut accumulator,
                        sink.as_ref(),
                        event,
                    )
                    .await
                    {
                        return evidence;
                    }
                }
                Some(Err(error))
                    if error
                        .validate_for(&context, provider.descriptor(), &request)
                        .is_ok() =>
                {
                    return ModelTerminalEvidence::Error(error);
                }
                Some(Err(_)) => {
                    return ModelTerminalEvidence::Error(model_contract_error(
                        &context,
                        provider.descriptor(),
                        ModelErrorPhase::Stream,
                        "runtime.model.invalid_stream_error",
                        "Model provider returned invalid stream failure evidence.",
                    ));
                }
                None => return finish_model_stream(&context, provider.descriptor(), accumulator),
            }
        }
    }

    async fn accept_model_stream_event(
        provider: &dyn stateknot_core::Model,
        context: &ModelContext,
        accumulator: &mut ModelEventAccumulator<'_>,
        sink: &dyn ModelEventSink,
        event: ModelEvent,
    ) -> Result<(), ModelTerminalEvidence> {
        if accumulator.push(event.clone()).is_err() {
            return Err(ModelTerminalEvidence::Error(model_contract_error(
                context,
                provider.descriptor(),
                ModelErrorPhase::Stream,
                "runtime.model.invalid_stream_event",
                "Model provider emitted an invalid semantic stream event.",
            )));
        }
        let emitted = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => {
                return Err(ModelTerminalEvidence::Error(model_stop_error(
                    context,
                    provider.descriptor(),
                    ModelStopReason::Cancelled,
                )));
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(context.deadline_instant())) => {
                return Err(ModelTerminalEvidence::Error(model_stop_error(
                    context,
                    provider.descriptor(),
                    ModelStopReason::DeadlineExceeded,
                )));
            }
            emitted = sink.emit(event) => emitted,
        };
        emitted.map_err(|_| {
            ModelTerminalEvidence::Error(model_contract_error(
                context,
                provider.descriptor(),
                ModelErrorPhase::Stream,
                "runtime.model.stream_sink_failed",
                "The durable model stream sink did not accept an event.",
            ))
        })
    }

    async fn dispatch_tool(
        &self,
        provider: Arc<dyn stateknot_core::ErasedTool>,
        context: ToolContext,
        input: stateknot_core::ToolInput,
    ) -> ToolTerminalEvidence {
        let result = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => {
                return ToolTerminalEvidence::Error(tool_stop_error(
                    &context,
                    provider.descriptor(),
                    ToolStopReason::Cancelled,
                ));
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(context.deadline_instant())) => {
                return ToolTerminalEvidence::Error(tool_stop_error(
                    &context,
                    provider.descriptor(),
                    ToolStopReason::DeadlineExceeded,
                ));
            }
            result = provider.call(context.clone(), input) => result,
        };
        match result {
            Ok(result) if result.validate_for(&context, provider.descriptor()).is_ok() => {
                ToolTerminalEvidence::Result(result)
            }
            Ok(_) => ToolTerminalEvidence::Error(tool_contract_error(
                &context,
                provider.descriptor(),
                ToolErrorPhase::Result,
                true,
                "runtime.tool.invalid_result",
                "Tool returned a result outside its registered contract.",
            )),
            Err(error) if error.validate_for(&context, provider.descriptor()).is_ok() => {
                ToolTerminalEvidence::Error(error)
            }
            Err(_) => ToolTerminalEvidence::Error(tool_contract_error(
                &context,
                provider.descriptor(),
                ToolErrorPhase::Execution,
                false,
                "runtime.tool.invalid_error",
                "Tool returned failure evidence outside its registered contract.",
            )),
        }
    }

    async fn advance_model_with_retry(
        &self,
        append: JournalAppend,
        expected: &ModelInvocation,
        transition: ModelInvocationTransition,
    ) -> Result<ModelInvocationCommitOutcome, StoreError> {
        let mut attempt = 1_u8;
        loop {
            match self
                .store
                .advance_model_invocation(append.clone(), &expected.head(), transition.clone())
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error)
                    if attempt < self.options.maximum_mutation_attempts()
                        && error.is_retryable() =>
                {
                    tokio::time::sleep(exponential_backoff(
                        self.options.mutation_retry_initial_delay(),
                        attempt,
                    ))
                    .await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn advance_tool_with_retry(
        &self,
        append: JournalAppend,
        expected: &ToolInvocation,
        transition: ToolInvocationTransition,
    ) -> Result<ToolInvocationCommitOutcome, StoreError> {
        let mut attempt = 1_u8;
        loop {
            match self
                .store
                .advance_tool_invocation(append.clone(), &expected.head(), transition.clone())
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error)
                    if attempt < self.options.maximum_mutation_attempts()
                        && error.is_retryable() =>
                {
                    tokio::time::sleep(exponential_backoff(
                        self.options.mutation_retry_initial_delay(),
                        attempt,
                    ))
                    .await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn event_payload(
        &self,
        kind: &'static str,
        operation: &'static str,
        binding_kind: &'static str,
        invocation_id: InvocationId,
        attempt_id: AttemptId,
        intent_digest: Digest,
    ) -> Result<JournalPayload, InvocationEventPayloadError> {
        let data = BoundedJson::try_from_value(json!({
            "operation": operation,
            "binding_kind": binding_kind,
            "invocation_id": invocation_id.to_string(),
            "attempt_id": attempt_id.to_string(),
            "intent_digest": digest_hex(intent_digest)
        }))
        .map_err(|_| InvocationEventPayloadError::Invalid)?;
        self.schemas
            .validate_bounded(&self.event_schema, &data)
            .map_err(|_| InvocationEventPayloadError::Invalid)?;
        JournalPayload::new(
            self.event_schema.clone(),
            JournalEventKind::new(kind).map_err(|_| InvocationEventPayloadError::Invalid)?,
            data,
        )
        .map_err(|_| InvocationEventPayloadError::Invalid)
    }
}

/// Startup failure for the durable invocation executor.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableInvocationExecutorBuildError {
    /// Embedded release schema was malformed.
    #[error(transparent)]
    EventSchema(#[from] StandardInvocationExecutionSchemaError),
    /// Frozen offline schema registry omitted the exact standard event schema.
    #[error("invocation execution event schema is absent from the frozen registry")]
    MissingEventSchema,
}

/// Failure before or around one model attempt dispatch.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ModelAttemptExecutionError {
    /// Handoff invariants changed before execution.
    #[error(transparent)]
    Handoff(#[from] InvocationAttemptHandoffError),
    /// No exact provider binding was installed.
    #[error(transparent)]
    Registry(#[from] ModelProviderRegistryError),
    /// Store read/start mutation failed before provider I/O.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Trusted run provenance crossed the retained fence.
    #[error("stored run provenance does not match the model attempt fence")]
    RunProvenanceMismatch,
    /// Runtime clock observation failed.
    #[error(transparent)]
    Clock(#[from] InvocationClockError),
    /// Trusted admission/accounting provider failed.
    #[error(transparent)]
    BudgetProvider(#[from] InvocationBudgetProviderError),
    /// Remaining capacity cannot admit the exact request snapshot.
    #[error("remaining run budget cannot admit the model attempt")]
    BudgetInsufficient,
    /// Finite model context could not be constructed.
    #[error(transparent)]
    Context(#[from] ModelContextError),
    /// Cancellation or deadline won before the durable start.
    #[error("model attempt stopped before durable start: {reason:?}")]
    StoppedBeforeStart {
        /// Deterministic stop reason.
        reason: ModelStopReason,
    },
    /// Standard public-safe journal payload could not be constructed.
    #[error("model invocation journal payload is invalid")]
    EventPayload(#[from] InvocationEventPayloadError),
    /// Worker journal append could not be constructed.
    #[error("model invocation worker journal append is invalid")]
    JournalAppend,
    /// Idempotent start recovery loaded another durable intent.
    #[error("recovered model invocation does not match the retained intent")]
    RecoveredInvocationMismatch,
    /// Another physical attempt advanced the invocation from this handoff.
    #[error("model invocation advanced under another physical attempt")]
    InvocationAdvanced,
    /// The store returned a future commit outcome unknown to this runtime.
    #[error("model invocation store outcome is unsupported by this runtime")]
    UnsupportedStoreOutcome,
    /// Provider I/O completed but terminal persistence needs a no-dispatch retry.
    #[error(transparent)]
    Terminal(#[from] ModelTerminalCommitError),
}

/// Failure before or around one tool attempt dispatch.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ToolAttemptExecutionError {
    /// Handoff invariants changed before execution.
    #[error(transparent)]
    Handoff(#[from] InvocationAttemptHandoffError),
    /// No exact executable tool binding was installed.
    #[error(transparent)]
    Registry(#[from] ToolProviderRegistryError),
    /// Store read/start mutation failed before tool I/O.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Trusted run provenance crossed the retained fence.
    #[error("stored run provenance does not match the tool attempt fence")]
    RunProvenanceMismatch,
    /// Runtime clock observation failed.
    #[error(transparent)]
    Clock(#[from] InvocationClockError),
    /// Trusted admission/accounting provider failed.
    #[error(transparent)]
    BudgetProvider(#[from] InvocationBudgetProviderError),
    /// Remaining capacity cannot admit the exact tool risk class.
    #[error("remaining run budget cannot admit the tool attempt")]
    BudgetInsufficient,
    /// Finite tool context could not be constructed.
    #[error(transparent)]
    Context(#[from] ToolContextError),
    /// Durable input no longer fits the trusted context/descriptor snapshot.
    #[error(transparent)]
    Input(#[from] ToolInputValidationError),
    /// Cancellation or deadline won before the durable start.
    #[error("tool attempt stopped before durable start: {reason:?}")]
    StoppedBeforeStart {
        /// Deterministic stop reason.
        reason: ToolStopReason,
    },
    /// Standard public-safe journal payload could not be constructed.
    #[error("tool invocation journal payload is invalid")]
    EventPayload(#[from] InvocationEventPayloadError),
    /// Worker journal append could not be constructed.
    #[error("tool invocation worker journal append is invalid")]
    JournalAppend,
    /// Idempotent start recovery loaded another durable intent.
    #[error("recovered tool invocation does not match the retained intent")]
    RecoveredInvocationMismatch,
    /// Another physical attempt advanced the invocation from this handoff.
    #[error("tool invocation advanced under another physical attempt")]
    InvocationAdvanced,
    /// The store returned a future commit outcome unknown to this runtime.
    #[error("tool invocation store outcome is unsupported by this runtime")]
    UnsupportedStoreOutcome,
    /// Tool I/O completed but terminal persistence needs a no-dispatch retry.
    #[error(transparent)]
    Terminal(#[from] ToolTerminalCommitError),
}

/// Invalid standard invocation journal event data.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum InvocationEventPayloadError {
    /// Bounds, pinned schema, event kind, or payload construction failed.
    #[error("standard invocation event payload is invalid")]
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invocation worker journal append is invalid")]
struct InvocationJournalAppendError;

fn validate_run_provenance(
    provenance: &AgentResultProvenance,
    fence: &RunFence,
) -> Result<(), ModelAttemptExecutionError> {
    if provenance.tenant_id() != fence.tenant_id() || provenance.run_id() != fence.run_id() {
        return Err(ModelAttemptExecutionError::RunProvenanceMismatch);
    }
    Ok(())
}

fn validate_tool_run_provenance(
    provenance: &AgentResultProvenance,
    fence: &RunFence,
) -> Result<(), ToolAttemptExecutionError> {
    if provenance.tenant_id() != fence.tenant_id() || provenance.run_id() != fence.run_id() {
        return Err(ToolAttemptExecutionError::RunProvenanceMismatch);
    }
    Ok(())
}

fn validate_model_budget(
    remaining: &BudgetRemaining,
    request: &ModelRequest,
) -> Result<(), ModelAttemptExecutionError> {
    if remaining.model_attempts() == ExecutionCount::ZERO
        || remaining.model_turns() == ExecutionCount::ZERO
        || remaining.input_tokens() < request.limits().max_input_tokens()
        || remaining.output_tokens() < request.limits().max_output_tokens()
        || remaining.input_bytes() < request.content_bytes()
    {
        return Err(ModelAttemptExecutionError::BudgetInsufficient);
    }
    Ok(())
}

fn validate_tool_budget(
    remaining: &BudgetRemaining,
    risk: ToolRisk,
) -> Result<(), ToolAttemptExecutionError> {
    if remaining.tool_calls() == ExecutionCount::ZERO
        || (risk != ToolRisk::ReadOnly && remaining.write_calls() == ExecutionCount::ZERO)
    {
        return Err(ToolAttemptExecutionError::BudgetInsufficient);
    }
    Ok(())
}

fn worker_append(
    fence: &RunFence,
    head: stateknot_core::JournalHead,
    event_id: EventId,
    payload: JournalPayload,
) -> Result<JournalAppend, InvocationJournalAppendError> {
    let intent = JournalEventIntent::worker(
        fence.tenant_id().clone(),
        fence.run_id(),
        event_id,
        fence.clone(),
        payload,
    )
    .map_err(|_| InvocationJournalAppendError)?;
    JournalAppend::new(JournalExpectation::exact(head), intent)
        .map_err(|_| InvocationJournalAppendError)
}

fn finish_model_stream(
    context: &ModelContext,
    descriptor: &stateknot_core::ModelDescriptor,
    accumulator: ModelEventAccumulator<'_>,
) -> ModelTerminalEvidence {
    match accumulator.finish() {
        Ok(response) => ModelTerminalEvidence::Response(response),
        Err(_) => ModelTerminalEvidence::Error(model_contract_error(
            context,
            descriptor,
            ModelErrorPhase::Stream,
            "runtime.model.incomplete_stream",
            "Model provider stream ended without a valid terminal event.",
        )),
    }
}

fn model_stop_error(
    context: &ModelContext,
    descriptor: &stateknot_core::ModelDescriptor,
    reason: ModelStopReason,
) -> ModelError {
    let (category, code, message) = match reason {
        ModelStopReason::Cancelled => (
            FailureCategory::Cancelled,
            "runtime.model.cancelled",
            "Model attempt stopped after cancellation was requested.",
        ),
        ModelStopReason::DeadlineExceeded => (
            FailureCategory::DeadlineExceeded,
            "runtime.model.deadline_exceeded",
            "Model attempt exceeded its effective deadline.",
        ),
        _ => (
            FailureCategory::Internal,
            "runtime.model.unsupported_stop_reason",
            "Model attempt stopped for a reason unsupported by this runtime.",
        ),
    };
    ModelError::new(
        runtime_failure(category, code, message, RetryAdvice::Never),
        ModelErrorPhase::Dispatch,
        ModelErrorProvenance::new(
            context.attempt_id(),
            descriptor.metadata().identity().clone(),
            None,
            None,
            None,
        ),
        None,
    )
}

fn model_contract_error(
    context: &ModelContext,
    descriptor: &stateknot_core::ModelDescriptor,
    phase: ModelErrorPhase,
    code: &'static str,
    message: &'static str,
) -> ModelError {
    ModelError::new(
        runtime_failure(FailureCategory::Internal, code, message, RetryAdvice::Never),
        phase,
        ModelErrorProvenance::new(
            context.attempt_id(),
            descriptor.metadata().identity().clone(),
            None,
            None,
            None,
        ),
        None,
    )
}

fn tool_stop_error(
    context: &ToolContext,
    descriptor: &stateknot_core::ToolDescriptor,
    reason: ToolStopReason,
) -> ToolError {
    let risk = descriptor.semantics().risk();
    let (failure, effect) = if risk == ToolRisk::ReadOnly {
        let (category, code, message) = match reason {
            ToolStopReason::Cancelled => (
                FailureCategory::Cancelled,
                "runtime.tool.cancelled",
                "Tool attempt stopped after cancellation was requested.",
            ),
            ToolStopReason::DeadlineExceeded => (
                FailureCategory::DeadlineExceeded,
                "runtime.tool.deadline_exceeded",
                "Tool attempt exceeded its effective deadline.",
            ),
            _ => (
                FailureCategory::Internal,
                "runtime.tool.unsupported_stop_reason",
                "Tool attempt stopped for a reason unsupported by this runtime.",
            ),
        };
        (
            runtime_failure(category, code, message, RetryAdvice::Never),
            ToolExternalEffect::NotApplicable,
        )
    } else {
        let (code, message) = match reason {
            ToolStopReason::Cancelled => (
                "runtime.tool.cancelled_outcome_unknown",
                "A cancelled tool write may have changed external state and requires reconciliation.",
            ),
            ToolStopReason::DeadlineExceeded => (
                "runtime.tool.deadline_outcome_unknown",
                "A timed-out tool write may have changed external state and requires reconciliation.",
            ),
            _ => (
                "runtime.tool.unsupported_stop_reason_outcome_unknown",
                "A stopped tool write may have changed external state and requires reconciliation.",
            ),
        };
        (
            runtime_failure(
                FailureCategory::AmbiguousExternalOutcome,
                code,
                message,
                RetryAdvice::ReconcileFirst,
            ),
            ToolExternalEffect::Unknown,
        )
    };
    ToolError::new(
        failure,
        ToolErrorPhase::Execution,
        effect,
        ToolErrorProvenance::for_invocation(context, descriptor),
    )
    .expect("runtime stop evidence is constructed from valid risk/effect pairs")
}

fn tool_contract_error(
    context: &ToolContext,
    descriptor: &stateknot_core::ToolDescriptor,
    phase: ToolErrorPhase,
    nominal_success: bool,
    code: &'static str,
    message: &'static str,
) -> ToolError {
    let risk = descriptor.semantics().risk();
    let (category, advice, effect) = match risk {
        ToolRisk::ReadOnly => (
            FailureCategory::Internal,
            RetryAdvice::Never,
            ToolExternalEffect::NotApplicable,
        ),
        ToolRisk::IdempotentWrite | ToolRisk::NonIdempotentWrite if nominal_success => (
            FailureCategory::Internal,
            RetryAdvice::Never,
            ToolExternalEffect::Applied,
        ),
        ToolRisk::IdempotentWrite | ToolRisk::NonIdempotentWrite => (
            FailureCategory::AmbiguousExternalOutcome,
            RetryAdvice::ReconcileFirst,
            ToolExternalEffect::Unknown,
        ),
    };
    ToolError::new(
        runtime_failure(category, code, message, advice),
        phase,
        effect,
        ToolErrorProvenance::for_invocation(context, descriptor),
    )
    .expect("runtime adapter-contract evidence is constructed from valid risk/effect pairs")
}

fn runtime_failure(
    category: FailureCategory,
    code: &'static str,
    message: &'static str,
    advice: RetryAdvice,
) -> Failure {
    Failure::new(
        FailureId::generate(),
        category,
        FailureCode::new(code).expect("runtime failure code is static and valid"),
        FailureOrigin::new("stateknot.runtime")
            .expect("runtime failure origin is static and valid"),
        FailureMessage::new(message).expect("runtime failure message is static and valid"),
        advice,
    )
    .expect("runtime failure category and advice are statically compatible")
}

fn exponential_backoff(initial: Duration, attempt: u8) -> Duration {
    let multiplier = 1_u32
        .checked_shl(u32::from(attempt.saturating_sub(1)))
        .unwrap_or(u32::MAX);
    initial
        .checked_mul(multiplier)
        .unwrap_or(MAX_MUTATION_RETRY_DELAY)
        .min(MAX_MUTATION_RETRY_DELAY)
}

fn digest_hex(digest: Digest) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(Digest::SHA256_LEN * 2);
    for byte in digest.as_bytes() {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_ids_and_retry_options_reject_unsafe_shapes() {
        let event = EventId::generate();
        assert_eq!(
            InvocationAttemptEventIds::new(event, event),
            Err(InvocationAttemptEventIdsError::Duplicate)
        );
        assert_eq!(
            DurableInvocationExecutorOptions::new(0, Duration::from_millis(1)),
            Err(DurableInvocationExecutorOptionsError::InvalidMutationAttempts)
        );
        assert_eq!(
            DurableInvocationExecutorOptions::new(1, Duration::ZERO),
            Err(DurableInvocationExecutorOptionsError::InvalidMutationRetryDelay)
        );
    }

    #[test]
    fn system_clock_produces_a_canonical_paired_observation() {
        let before = Instant::now();
        let observation = SystemInvocationClock.observe().unwrap();
        assert!(observation.observed_at().unix_micros() > 0);
        assert!(observation.observed_instant() >= before);
    }
}
