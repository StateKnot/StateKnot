// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{str::FromStr, time::Duration};

use sqlx_postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use crate::ConfigurationError;

const MAX_POSTGRES_TIMEOUT_MILLISECONDS: u128 = i32::MAX as u128;

/// Database transport policy applied after parsing the connection URL.
///
/// The default is certificate and hostname verification. Disabling TLS is an
/// explicit choice intended only for a trusted local socket or isolated test
/// network; URL query parameters cannot silently weaken this setting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostgresTransportSecurity {
    /// Require encryption and verify the certificate chain and hostname.
    #[default]
    VerifyFull,
    /// Require encryption but do not verify the server identity.
    RequireEncryption,
    /// Disable TLS for an explicitly trusted local or isolated test network.
    Disabled,
}

impl PostgresTransportSecurity {
    const fn ssl_mode(self) -> PgSslMode {
        match self {
            Self::VerifyFull => PgSslMode::VerifyFull,
            Self::RequireEncryption => PgSslMode::Require,
            Self::Disabled => PgSslMode::Disable,
        }
    }
}

/// Bounded connection, transaction, and lease settings for [`crate::PostgresStore`].
#[derive(Clone, Debug)]
pub struct PostgresStoreOptions {
    pub(crate) transport_security: PostgresTransportSecurity,
    pub(crate) max_connections: u32,
    pub(crate) min_connections: u32,
    pub(crate) acquire_timeout: Duration,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) lock_timeout: Duration,
    pub(crate) statement_timeout: Duration,
    pub(crate) lease_duration: Duration,
    pub(crate) maximum_lease_horizon: Duration,
}

impl PostgresStoreOptions {
    /// Returns the configured transport policy.
    #[must_use]
    pub const fn transport_security(&self) -> PostgresTransportSecurity {
        self.transport_security
    }

    /// Overrides the transport policy.
    #[must_use]
    pub const fn with_transport_security(
        mut self,
        transport_security: PostgresTransportSecurity,
    ) -> Self {
        self.transport_security = transport_security;
        self
    }

    /// Sets the inclusive pool-size bounds.
    #[must_use]
    pub const fn with_pool_size(mut self, minimum: u32, maximum: u32) -> Self {
        self.min_connections = minimum;
        self.max_connections = maximum;
        self
    }

    /// Sets the maximum wait for a pooled connection.
    #[must_use]
    pub const fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Sets the idle connection lifetime, or disables idle retirement.
    #[must_use]
    pub const fn with_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Sets the absolute connection lifetime, or disables lifetime retirement.
    #[must_use]
    pub const fn with_max_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Sets per-transaction `PostgreSQL` lock and statement timeouts.
    #[must_use]
    pub const fn with_transaction_timeouts(
        mut self,
        lock_timeout: Duration,
        statement_timeout: Duration,
    ) -> Self {
        self.lock_timeout = lock_timeout;
        self.statement_timeout = statement_timeout;
        self
    }

    /// Sets initial lease duration and the maximum accepted renewal horizon.
    #[must_use]
    pub const fn with_lease_timing(
        mut self,
        lease_duration: Duration,
        maximum_lease_horizon: Duration,
    ) -> Self {
        self.lease_duration = lease_duration;
        self.maximum_lease_horizon = maximum_lease_horizon;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigurationError> {
        if self.max_connections == 0 {
            return Err(ConfigurationError::ZeroMaximumConnections);
        }
        if self.min_connections > self.max_connections {
            return Err(ConfigurationError::PoolMinimumExceedsMaximum);
        }
        for (name, duration) in [
            ("acquire timeout", self.acquire_timeout),
            ("lock timeout", self.lock_timeout),
            ("statement timeout", self.statement_timeout),
            ("lease duration", self.lease_duration),
            ("maximum lease horizon", self.maximum_lease_horizon),
        ] {
            validate_nonzero_duration(name, duration)?;
        }
        validate_postgres_timeout("lock timeout", self.lock_timeout)?;
        validate_postgres_timeout("statement timeout", self.statement_timeout)?;
        if duration_millisecond_count(self.lock_timeout)
            >= duration_millisecond_count(self.statement_timeout)
        {
            return Err(ConfigurationError::LockTimeoutNotBelowStatementTimeout);
        }
        validate_lease_timing("lease duration", self.lease_duration)?;
        validate_lease_timing("maximum lease horizon", self.maximum_lease_horizon)?;
        if self.lease_duration > self.maximum_lease_horizon {
            return Err(ConfigurationError::LeaseDurationExceedsMaximumHorizon);
        }
        if let Some(duration) = self.idle_timeout {
            validate_nonzero_duration("idle timeout", duration)?;
        }
        if let Some(duration) = self.max_lifetime {
            validate_nonzero_duration("maximum connection lifetime", duration)?;
        }
        Ok(())
    }

    pub(crate) fn connect_options(
        &self,
        database_url: &str,
    ) -> Result<PgConnectOptions, ConfigurationError> {
        self.validate()?;
        let options = PgConnectOptions::from_str(database_url)
            .map_err(|_| ConfigurationError::InvalidDatabaseUrl)?
            .ssl_mode(self.transport_security.ssl_mode())
            .options([("search_path", "public,pg_catalog")])
            .application_name("stateknot");
        Ok(options)
    }

    pub(crate) fn pool_options(&self) -> PgPoolOptions {
        let mut options = PgPoolOptions::new()
            .min_connections(self.min_connections)
            .max_connections(self.max_connections)
            .acquire_timeout(self.acquire_timeout)
            .idle_timeout(self.idle_timeout)
            .max_lifetime(self.max_lifetime);
        options = options.test_before_acquire(true);
        options
    }

    pub(crate) fn lock_timeout_setting(&self) -> String {
        duration_milliseconds(self.lock_timeout)
    }

    pub(crate) fn statement_timeout_setting(&self) -> String {
        duration_milliseconds(self.statement_timeout)
    }
}

impl Default for PostgresStoreOptions {
    fn default() -> Self {
        Self {
            transport_security: PostgresTransportSecurity::VerifyFull,
            max_connections: 16,
            min_connections: 1,
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            lock_timeout: Duration::from_secs(5),
            statement_timeout: Duration::from_secs(15),
            lease_duration: Duration::from_secs(30),
            maximum_lease_horizon: Duration::from_secs(5 * 60),
        }
    }
}

fn validate_nonzero_duration(
    name: &'static str,
    duration: Duration,
) -> Result<(), ConfigurationError> {
    if duration.is_zero() {
        return Err(ConfigurationError::ZeroDuration { name });
    }
    if duration.as_micros() > i64::MAX as u128 {
        return Err(ConfigurationError::DurationTooLarge { name });
    }
    Ok(())
}

fn duration_milliseconds(duration: Duration) -> String {
    let milliseconds = duration_millisecond_count(duration);
    format!("{milliseconds}ms")
}

fn duration_millisecond_count(duration: Duration) -> u128 {
    duration.as_nanos().div_ceil(1_000_000)
}

fn validate_postgres_timeout(
    name: &'static str,
    duration: Duration,
) -> Result<(), ConfigurationError> {
    if duration_millisecond_count(duration) > MAX_POSTGRES_TIMEOUT_MILLISECONDS {
        return Err(ConfigurationError::PostgresTimeoutTooLarge { name });
    }
    Ok(())
}

fn validate_lease_timing(name: &'static str, duration: Duration) -> Result<(), ConfigurationError> {
    if duration.as_micros() == 0 || duration.subsec_nanos() % 1_000 != 0 {
        return Err(ConfigurationError::LeaseTimingNotMicrosecondAligned { name });
    }
    Ok(())
}
