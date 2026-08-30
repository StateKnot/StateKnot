// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

/// Invalid `PostgreSQL` provider configuration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ConfigurationError {
    /// The connection URL could not be parsed. The URL is intentionally omitted.
    #[error("PostgreSQL connection URL is invalid")]
    InvalidDatabaseUrl,
    /// A pool with zero possible connections cannot make progress.
    #[error("PostgreSQL maximum connections must be positive")]
    ZeroMaximumConnections,
    /// Minimum pool size exceeded maximum pool size.
    #[error("PostgreSQL minimum connections must not exceed maximum connections")]
    PoolMinimumExceedsMaximum,
    /// A required timeout or lease duration was zero.
    #[error("{name} must be positive")]
    ZeroDuration {
        /// Stable configuration field name.
        name: &'static str,
    },
    /// A duration could not fit the signed microsecond database contract.
    #[error("{name} exceeds the signed microsecond storage range")]
    DurationTooLarge {
        /// Stable configuration field name.
        name: &'static str,
    },
    /// Lock timeout would not fire strictly before the statement timeout.
    #[error("lock timeout must be strictly less than statement timeout")]
    LockTimeoutNotBelowStatementTimeout,
    /// A server-side timeout exceeded `PostgreSQL`'s signed millisecond range.
    #[error("{name} exceeds PostgreSQL's signed millisecond timeout range")]
    PostgresTimeoutTooLarge {
        /// Stable configuration field name.
        name: &'static str,
    },
    /// Durable lease timing was not exactly representable as microseconds.
    #[error("{name} must be a positive whole number of microseconds")]
    LeaseTimingNotMicrosecondAligned {
        /// Stable configuration field name.
        name: &'static str,
    },
    /// Initial leases exceeded the configured renewal safety horizon.
    #[error("lease duration must not exceed the maximum lease horizon")]
    LeaseDurationExceedsMaximumHorizon,
    /// Fixed outbox attempt lease exceeded the core five-minute bound.
    #[error("outbox attempt lease must not exceed five minutes")]
    OutboxAttemptLeaseTooLong,
}

/// Durable `PostgreSQL` operation failure with payload-redacted diagnostics.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Provider configuration was invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// Connection or SQL execution failed.
    #[error("PostgreSQL {operation} failed")]
    Database {
        /// Stable operation category without SQL or bound values.
        operation: &'static str,
        /// Underlying driver error.
        #[source]
        source: sqlx_core::Error,
    },
    /// Ordered schema migration failed.
    #[error("PostgreSQL schema migration failed")]
    Migration {
        /// Underlying migration error.
        #[source]
        source: sqlx_core::migrate::MigrateError,
    },
    /// The connected server is outside the qualified major versions.
    #[error("PostgreSQL server major version is unsupported")]
    UnsupportedServerVersion,
    /// Required migration metadata has not been installed.
    #[error("StateKnot PostgreSQL schema has not been migrated")]
    SchemaNotMigrated,
    /// Applied migration versions or checksums do not match this binary.
    #[error("StateKnot PostgreSQL schema is incompatible with this binary")]
    IncompatibleSchema,
    /// Migration metadata exists but required schema objects are missing.
    #[error("StateKnot PostgreSQL schema objects are incomplete")]
    IncompleteSchema,
    /// No run exists inside the supplied tenant boundary.
    #[error("run was not found in the tenant boundary")]
    RunNotFound,
    /// Run admission reused an identity with different provenance.
    #[error("run identity already exists with different admission provenance")]
    RunConflict,
    /// The run is quarantined and cannot execute or mutate normally.
    #[error("run is quarantined")]
    RunQuarantined,
    /// A quarantine component code violated its bounded machine grammar.
    #[error("run quarantine component code is invalid")]
    InvalidRunQuarantineComponent,
    /// A quarantine request crossed its tenant/run journal observation.
    #[error("run quarantine request is invalid")]
    InvalidRunQuarantineRequest,
    /// No audit-grade quarantine observation exists for the supplied run.
    #[error("run quarantine evidence was not found in the tenant-scoped run")]
    RunQuarantineNotFound,
    /// A stable quarantine identity was reused for another immutable intent.
    #[error("run quarantine identity conflicts with durable evidence")]
    RunQuarantineIdConflict,
    /// Another immutable quarantine observation already owns the run.
    #[error("run already has different durable quarantine evidence")]
    RunQuarantineConflict,
    /// The run journal changed after the evidence observation was constructed.
    #[error("run quarantine journal observation is stale")]
    StaleRunQuarantineObservation,
    /// A claimed recovery session crossed its fence, tenant, or run boundary.
    #[error("claimed run recovery context is invalid")]
    InvalidClaimedRunRecoveryContext,
    /// Durable journal progress invalidated the claimed recovery observation.
    #[error("claimed run recovery journal observation is stale")]
    StaleClaimedRunRecoveryObservation,
    /// Ready-node recovery was requested before an initial checkpoint exists.
    #[error("ready-node recovery requires a current graph checkpoint")]
    ReadyNodeRecoveryCheckpointMissing,
    /// A recovery plan, journal append, fence, or node selection crossed scope.
    #[error("ready-node dispatch plan is invalid for the requested durable start")]
    InvalidReadyNodeDispatchPlan,
    /// The selected recovery-plan node does not authorize a new physical start.
    #[error("ready-node recovery decision is not dispatchable")]
    ReadyNodeNotDispatchable,
    /// Immutable quarantine evidence and the mutable run projection disagree.
    #[error("run quarantine evidence conflicts with its durable projection")]
    RunQuarantineCommitConflict,
    /// The run lifecycle is not currently runnable.
    #[error("run lifecycle is not runnable")]
    RunNotRunnable,
    /// A caller reached the wrong trusted append entry point.
    #[error("journal append source is not authorized by this entry point")]
    WrongAppendAuthority,
    /// A stable event ID was reused for a different immutable intent.
    #[error("journal event identity conflicts with a committed intent")]
    EventIdConflict,
    /// A retry reused an event ID with a different or unverifiable projection intent.
    #[error("journal event projection conflicts with the committed projection intent")]
    ProjectionIntentConflict,
    /// The supplied complete journal head did not match the locked run row.
    #[error("journal expectation does not match the current durable head")]
    StaleJournalHead,
    /// No checkpoint exists for the supplied tenant/run/checkpoint identity.
    #[error("checkpoint was not found in the tenant-scoped run")]
    CheckpointNotFound,
    /// A stable checkpoint ID was reused for a different immutable write intent.
    #[error("checkpoint identity conflicts with a committed write intent")]
    CheckpointIdConflict,
    /// Event and checkpoint idempotency records do not describe one atomic commit.
    #[error("journal event and checkpoint identities conflict with an atomic commit")]
    CheckpointCommitConflict,
    /// A wait batch was empty, oversized, duplicated, or crossed scope/event identity.
    #[error("durable wait registration batch is invalid")]
    InvalidWaitRegistrationBatch,
    /// Wait event, checkpoint, lifecycle markers, and registration rows disagree.
    #[error("durable wait registration conflicts with the atomic commit")]
    WaitRegistrationCommitConflict,
    /// No durable wait registration exists under the supplied tenant/run/identity.
    #[error("durable wait registration was not found in the tenant-scoped run")]
    WaitRegistrationNotFound,
    /// A wait identity was loaded through the wrong interrupt/timer API.
    #[error("durable wait registration kind does not match the requested API")]
    WaitRegistrationKindMismatch,
    /// Resolution intent did not match an outstanding durable interrupt.
    #[error("interrupt resolution is invalid for the locked durable request")]
    InvalidInterruptResolution,
    /// Resolution event, immutable detail, and mutable wait projection disagree.
    #[error("interrupt resolution conflicts with the atomic commit")]
    InterruptResolutionCommitConflict,
    /// Timer firing intent did not match an outstanding due durable timer.
    #[error("timer firing is invalid for the locked durable timer")]
    InvalidTimerFiring,
    /// Firing event, immutable detail, and mutable wait projection disagree.
    #[error("timer firing conflicts with the atomic commit")]
    TimerFiringCommitConflict,
    /// A wait-discovery page size was zero or exceeded the decoded-memory bound.
    #[error("wait discovery page size is invalid")]
    InvalidWaitDiscoveryPageSize,
    /// A due-timer page cursor crossed tenant/cutoff/order scope.
    #[error("due timer page cursor is invalid")]
    InvalidDueTimerCursor,
    /// An expired-interrupt page cursor crossed tenant/cutoff/order scope.
    #[error("expired interrupt page cursor is invalid")]
    InvalidExpiredInterruptCursor,
    /// The supplied run-level edge was not cancellation/failure from waiting.
    #[error("wait abandonment transition is invalid")]
    InvalidWaitAbandonment,
    /// Abandonment event, detail set, and mutable wait projections disagree.
    #[error("wait abandonment conflicts with the atomic commit")]
    WaitAbandonmentCommitConflict,
    /// The requested wait has no immutable abandonment audit fact.
    #[error("wait abandonment was not found in the tenant-scoped run")]
    WaitAbandonmentNotFound,
    /// Resolution/firing history was requested for a wait abandoned by the run.
    #[error("durable wait was abandoned without resolution or firing")]
    WaitWasAbandoned,
    /// A successor checkpoint must prove the complete result barrier it consumes.
    #[error("successor checkpoint commit requires a complete checkpoint barrier")]
    CheckpointBarrierRequired,
    /// The supplied barrier did not match its fully restored durable base.
    #[error("checkpoint barrier does not match its durable base checkpoint")]
    InvalidCheckpointBarrier,
    /// At least one result required by the complete barrier is not durable.
    #[error("checkpoint barrier is incomplete")]
    CheckpointBarrierIncomplete,
    /// Durable pending-result identities do not equal the supplied complete barrier.
    #[error("checkpoint barrier result set conflicts with durable pending results")]
    CheckpointBarrierResultConflict,
    /// Barrier event, checkpoint, and consumption rows do not describe one atomic commit.
    #[error("checkpoint barrier identities conflict with an atomic commit")]
    CheckpointBarrierCommitConflict,
    /// The supplied complete checkpoint parent did not match the locked run row.
    #[error("checkpoint parent does not match the current durable checkpoint")]
    StaleCheckpointHead,
    /// A tool invocation on the current checkpoint has not committed a result.
    #[error("checkpoint advancement is blocked by an unsettled tool invocation")]
    CheckpointBlockedByToolInvocation,
    /// No logical tool invocation exists in the supplied tenant/run boundary.
    #[error("tool invocation was not found in the tenant-scoped run")]
    ToolInvocationNotFound,
    /// A stable invocation ID was reused with a different immutable intent.
    #[error("tool invocation identity conflicts with a committed intent")]
    ToolInvocationIdConflict,
    /// Event and invocation records do not describe one atomic commit.
    #[error("journal event and tool invocation identities conflict with an atomic commit")]
    ToolInvocationCommitConflict,
    /// The supplied complete invocation head did not match its durable current row.
    #[error("tool invocation head is stale")]
    StaleToolInvocationHead,
    /// The requested invocation state transition was invalid for durable state.
    #[error("tool invocation transition is invalid for the locked record")]
    InvalidToolInvocationTransition,
    /// The invocation activation is not a ready root-graph node of its checkpoint.
    #[error("tool invocation activation is not ready in the base checkpoint")]
    InvalidToolInvocationActivation,
    /// A model invocation on the current checkpoint has not committed a response.
    #[error("checkpoint advancement is blocked by an unsettled model invocation")]
    CheckpointBlockedByModelInvocation,
    /// No logical model invocation exists in the supplied tenant/run boundary.
    #[error("model invocation was not found in the tenant-scoped run")]
    ModelInvocationNotFound,
    /// A stable model invocation ID was reused with a different immutable intent.
    #[error("model invocation identity conflicts with a committed intent")]
    ModelInvocationIdConflict,
    /// Event and model invocation records do not describe one atomic commit.
    #[error("journal event and model invocation identities conflict with an atomic commit")]
    ModelInvocationCommitConflict,
    /// The supplied complete model invocation head did not match its durable current row.
    #[error("model invocation head is stale")]
    StaleModelInvocationHead,
    /// The requested model invocation transition was invalid for durable state.
    #[error("model invocation transition is invalid for the locked record")]
    InvalidModelInvocationTransition,
    /// The model invocation activation is not a ready root-graph node of its checkpoint.
    #[error("model invocation activation is not ready in the base checkpoint")]
    InvalidModelInvocationActivation,
    /// No pending result exists for the supplied logical node activation key.
    #[error("pending node result was not found in the tenant-scoped run")]
    PendingNodeResultNotFound,
    /// A logical activation already has a different immutable semantic result.
    #[error("pending node result conflicts with the committed semantic intent")]
    PendingNodeResultConflict,
    /// Event and pending-result rows do not describe one atomic commit.
    #[error("journal event and pending node result conflict with an atomic commit")]
    PendingNodeResultCommitConflict,
    /// The pending result activation is not a ready root-graph node of its checkpoint.
    #[error("pending node result activation is not ready in the base checkpoint")]
    InvalidPendingNodeResultActivation,
    /// New pending results must be committed through a durable physical node attempt.
    #[error("pending node result commit requires a durable node attempt")]
    NodeAttemptRequired,
    /// The result's temporal or control invariants cannot commit at the database observation.
    #[error("pending node result is invalid at its durable commit position")]
    InvalidPendingNodeResult,
    /// A pending result named an absent, non-committed, or crossed invocation revision.
    #[error("pending node result contains an invalid external invocation binding")]
    InvalidPendingNodeResultBinding,
    /// A pending-result page size exceeded its decoded-memory safety bound.
    #[error("pending node result page size is invalid")]
    InvalidPendingNodeResultPageSize,
    /// A pending-result page cursor crossed scope or did not match its durable row.
    #[error("pending node result cursor is invalid")]
    InvalidPendingNodeResultCursor,
    /// No physical node attempt exists in the supplied tenant/run boundary.
    #[error("node attempt was not found in the tenant-scoped run")]
    NodeAttemptNotFound,
    /// A stable physical attempt identity was reused for another owner.
    #[error("node attempt identity conflicts with a committed owner")]
    NodeAttemptIdConflict,
    /// Event and node-attempt rows do not describe one atomic commit.
    #[error("journal event and node attempt conflict with an atomic commit")]
    NodeAttemptCommitConflict,
    /// The attempt activation is not a ready root-graph node of its checkpoint.
    #[error("node attempt activation is not ready in the base checkpoint")]
    InvalidNodeAttemptActivation,
    /// The requested physical-attempt transition is invalid for durable history.
    #[error("node attempt transition is invalid for the locked history")]
    InvalidNodeAttemptTransition,
    /// One logical activation reached the hard physical-attempt ceiling.
    #[error("node attempt history reached its hard physical-attempt limit")]
    NodeAttemptLimitExceeded,
    /// The supplied exact start does not match the durable physical attempt.
    #[error("node attempt start is stale")]
    StaleNodeAttemptStart,
    /// A node-attempt history cursor did not identify the exact stored attempt.
    #[error("node attempt history cursor does not match a durable attempt")]
    InvalidNodeAttemptCursor,
    /// A node-attempt history page size exceeded its decoded-memory safety bound.
    #[error("node attempt history page size is invalid")]
    InvalidNodeAttemptPageSize,
    /// A runnable-run page size exceeded its decoded-memory safety bound.
    #[error("runnable run page size is invalid")]
    InvalidRunnableRunPageSize,
    /// A runnable-run cursor crossed tenant scope or contained an invalid key.
    #[error("runnable run cursor is invalid")]
    InvalidRunnableRunCursor,
    /// An outbox enqueue batch was empty, duplicated, or exceeded its hard bound.
    #[error("outbox enqueue batch is invalid")]
    InvalidOutboxBatch,
    /// Destination snapshot digest did not match its canonical configuration.
    #[error("outbox destination snapshot does not match its canonical configuration")]
    OutboxDestinationSnapshotMismatch,
    /// No immutable destination snapshot exists in the tenant boundary.
    #[error("outbox destination snapshot was not found in the tenant boundary")]
    OutboxDestinationNotFound,
    /// A destination snapshot identity conflicts with durable configuration.
    #[error("outbox destination snapshot conflicts with durable configuration")]
    OutboxDestinationConflict,
    /// No outbox delivery exists in the supplied tenant/run boundary.
    #[error("outbox delivery was not found in the tenant-scoped run")]
    OutboxDeliveryNotFound,
    /// A stable delivery identity conflicts with a committed immutable intent.
    #[error("outbox delivery identity conflicts with a committed intent")]
    OutboxDeliveryIdConflict,
    /// Journal event and complete outbox set do not describe one atomic commit.
    #[error("journal event and outbox deliveries conflict with an atomic commit")]
    OutboxEnqueueConflict,
    /// A physical outbox attempt identity was already claimed by another owner.
    #[error("outbox attempt identity conflicts with a committed owner")]
    OutboxAttemptIdConflict,
    /// The supplied delivery fence is not the exact current attempt.
    #[error("outbox delivery fencing token is stale")]
    StaleOutboxFence,
    /// The current outbox attempt reached its exclusive database expiry.
    #[error("outbox attempt lease has expired")]
    OutboxAttemptExpired,
    /// A delivery attempt has no durable start in the tenant-scoped run.
    #[error("outbox attempt was not found in the tenant-scoped run")]
    OutboxAttemptNotFound,
    /// A completion conflicts with an already committed terminal attempt.
    #[error("outbox attempt completion conflicts with durable evidence")]
    OutboxCompletionConflict,
    /// The requested outbox state transition is invalid for durable history.
    #[error("outbox attempt transition is invalid for the locked history")]
    InvalidOutboxTransition,
    /// An outbox history cursor did not identify the exact stored attempt.
    #[error("outbox attempt history cursor does not match a durable attempt")]
    InvalidOutboxAttemptCursor,
    /// An outbox-attempt page size exceeded its decoded-memory safety bound.
    #[error("outbox attempt history page size is invalid")]
    InvalidOutboxAttemptPageSize,
    /// The run journal advanced after an earlier pending-result page snapshot.
    #[error("pending node result page snapshot is stale and must be restarted")]
    StalePendingNodeResultSnapshot,
    /// A pagination cursor did not identify the exact stored event head.
    #[error("journal cursor does not match a durable event")]
    InvalidJournalCursor,
    /// A reverse-lineage cursor did not identify the exact stored checkpoint head.
    #[error("checkpoint lineage cursor does not match a durable checkpoint")]
    InvalidCheckpointCursor,
    /// An invocation-history cursor did not identify the exact stored revision.
    #[error("tool invocation history cursor does not match a durable revision")]
    InvalidToolInvocationCursor,
    /// A model-invocation history cursor did not identify the exact stored revision.
    #[error("model invocation history cursor does not match a durable revision")]
    InvalidModelInvocationCursor,
    /// No higher signed `PostgreSQL` journal sequence exists.
    #[error("journal sequence is exhausted")]
    JournalSequenceExhausted,
    /// The expected lifecycle revision did not match the locked run row.
    #[error("lifecycle projection revision is stale")]
    StaleLifecycleRevision,
    /// The requested lifecycle transition was not a valid direct successor.
    #[error("lifecycle transition is invalid for the locked run state")]
    InvalidLifecycleTransition,
    /// A durable-wait edge was attempted through a journal-only mutation path.
    #[error("durable wait lifecycle changes require the atomic wait persistence API")]
    DurableWaitMutationRequired,
    /// A lifecycle observation claimed to occur after the journal commit clock.
    #[error("lifecycle transition observation is later than the journal commit observation")]
    LifecycleObservationAfterCommit,
    /// Another unexpired worker lease currently owns the run.
    #[error("run already has an unexpired worker lease")]
    LeaseHeld,
    /// No active lease can satisfy the requested mutation.
    #[error("run has no active worker lease")]
    NoActiveLease,
    /// Attempt or fencing epoch did not match the current lease.
    #[error("worker fencing token is stale")]
    StaleFence,
    /// The exact lease reached its exclusive database expiry.
    #[error("worker lease has expired")]
    LeaseExpired,
    /// The database wall clock moved before a durable lease observation.
    #[error("database clock precedes a durable lease observation")]
    DatabaseClockRegression,
    /// Renewal did not strictly extend the current expiry.
    #[error("worker lease renewal must strictly extend the current expiry")]
    LeaseExpiryNotExtended,
    /// Renewal requested an expiry beyond the configured safety horizon.
    #[error("worker lease renewal exceeds the configured maximum horizon")]
    LeaseHorizonExceeded,
    /// No higher signed `PostgreSQL` fencing epoch exists.
    #[error("worker fencing epoch is exhausted")]
    FencingEpochExhausted,
    /// A requested journal page exceeded the hard bound.
    #[error("journal page size is invalid")]
    InvalidPageSize,
    /// A requested checkpoint-lineage page exceeded its memory-safety bound.
    #[error("checkpoint lineage page size is invalid")]
    InvalidCheckpointPageSize,
    /// A requested invocation-history page exceeded its memory-safety bound.
    #[error("tool invocation history page size is invalid")]
    InvalidToolInvocationPageSize,
    /// A requested model-invocation history page exceeded its memory-safety bound.
    #[error("model invocation history page size is invalid")]
    InvalidModelInvocationPageSize,
    /// Trusted in-process domain data could not be serialized canonically.
    #[error("{record} could not be encoded for durable storage")]
    Encoding {
        /// Stable record category without user data.
        record: &'static str,
    },
    /// Durable bytes or redundant columns failed closed validation.
    #[error("durable {record} failed integrity validation")]
    CorruptData {
        /// Stable record category without payload or identifiers.
        record: &'static str,
    },
}

impl StoreError {
    pub(crate) const fn database(operation: &'static str, source: sqlx_core::Error) -> Self {
        Self::Database { operation, source }
    }

    pub(crate) const fn corrupt(record: &'static str) -> Self {
        Self::CorruptData { record }
    }

    pub(crate) const fn encoding(record: &'static str) -> Self {
        Self::Encoding { record }
    }

    /// Returns the stable, payload-free record category for an integrity error.
    ///
    /// Recovery code can map this value to a bounded quarantine component and
    /// hash its separately retained evidence without exposing durable payloads.
    #[must_use]
    pub const fn corrupt_record(&self) -> Option<&'static str> {
        match self {
            Self::CorruptData { record } => Some(record),
            _ => None,
        }
    }

    /// Returns whether retrying the complete transaction with identical stable
    /// identities is safe after this failure category.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Database { source, .. } => retryable_database_error(source),
            _ => false,
        }
    }
}

fn retryable_database_error(error: &sqlx_core::Error) -> bool {
    match error {
        sqlx_core::Error::Io(_)
        | sqlx_core::Error::Tls(_)
        | sqlx_core::Error::PoolTimedOut
        | sqlx_core::Error::PoolClosed
        | sqlx_core::Error::WorkerCrashed => true,
        sqlx_core::Error::Database(database) => database.code().is_some_and(|code| {
            matches!(
                code.as_ref(),
                "40001" | "40P01" | "55P03" | "57014" | "57P01" | "57P02" | "57P03"
            )
        }),
        _ => false,
    }
}
