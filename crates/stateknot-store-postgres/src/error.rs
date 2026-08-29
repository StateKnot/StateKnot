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
    /// The supplied complete checkpoint parent did not match the locked run row.
    #[error("checkpoint parent does not match the current durable checkpoint")]
    StaleCheckpointHead,
    /// A pagination cursor did not identify the exact stored event head.
    #[error("journal cursor does not match a durable event")]
    InvalidJournalCursor,
    /// No higher signed `PostgreSQL` journal sequence exists.
    #[error("journal sequence is exhausted")]
    JournalSequenceExhausted,
    /// The expected lifecycle revision did not match the locked run row.
    #[error("lifecycle projection revision is stale")]
    StaleLifecycleRevision,
    /// The requested lifecycle transition was not a valid direct successor.
    #[error("lifecycle transition is invalid for the locked run state")]
    InvalidLifecycleTransition,
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
