// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{env, fmt, str::FromStr, time::Duration};

use sqlx_postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use crate::ConfigurationError;

const MAX_POSTGRES_TIMEOUT_MILLISECONDS: u128 = i32::MAX as u128;

/// Runtime connection URL read by [`PostgresStoreConfig::from_env`].
pub const DATABASE_URL_ENV: &str = "DATABASE_URL";
/// Optional deployment-authorized migration URL read by
/// [`PostgresStoreConfig::from_env`].
pub const MIGRATION_DATABASE_URL_ENV: &str = "STATEKNOT_MIGRATION_DATABASE_URL";
/// Explicit development-profile switch read by [`PostgresStoreConfig::from_env`].
pub const DEV_MODE_ENV: &str = "STATEKNOT_DEV_MODE";
/// Explicit migration switch read by [`PostgresStoreConfig::from_env`].
pub const AUTO_MIGRATE_ENV: &str = "STATEKNOT_AUTO_MIGRATE";

/// Security profile selected for a complete store connection configuration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostgresConfigurationProfile {
    /// Production defaults: verified TLS and no implicit schema migration.
    #[default]
    Production,
    /// Explicit local-development defaults with bounded resources.
    Development,
}

/// Complete, validated store startup configuration.
///
/// Connection URLs are intentionally omitted from [`Debug`](fmt::Debug) and
/// have no public accessor. Use [`PostgresStoreConfig::builder`] for production
/// or [`PostgresStoreConfig::development`] for an explicit local-development
/// profile.
#[derive(Clone)]
pub struct PostgresStoreConfig {
    runtime_database_url: Box<str>,
    migration_database_url: Option<Box<str>>,
    options: PostgresStoreOptions,
    auto_migrate: bool,
    profile: PostgresConfigurationProfile,
}

impl PostgresStoreConfig {
    /// Starts a production-safe builder for one runtime connection URL.
    #[must_use]
    pub fn builder(database_url: impl Into<String>) -> PostgresStoreConfigBuilder {
        PostgresStoreConfigBuilder::new(database_url)
    }

    /// Builds an explicit local-development profile.
    ///
    /// This profile disables transport security, limits the pool to four
    /// connections, and enables migration through the same URL. It must never
    /// be used across an untrusted network or as a production deployment
    /// shortcut.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError`] when the URL or derived options are
    /// invalid.
    pub fn development(database_url: impl Into<String>) -> Result<Self, ConfigurationError> {
        PostgresStoreConfigBuilder::new(database_url)
            .with_development_defaults()
            .build()
    }

    /// Reads a complete configuration from the process environment.
    ///
    /// `DATABASE_URL` is required. Production is the default profile. Setting
    /// `STATEKNOT_DEV_MODE=true` explicitly selects the development profile;
    /// setting `STATEKNOT_AUTO_MIGRATE=true` in production additionally
    /// requires a distinct `STATEKNOT_MIGRATION_DATABASE_URL`.
    ///
    /// Boolean values accept only `true` or `false`, ignoring ASCII case. URLs
    /// are never included in returned error messages.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError`] for missing/non-Unicode variables,
    /// invalid booleans, unsafe migration configuration, or malformed URLs.
    pub fn from_env() -> Result<Self, ConfigurationError> {
        Self::from_environment(|name| match env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(env::VarError::NotUnicode(_)) => Err(()),
        })
    }

    fn from_environment(
        mut read: impl FnMut(&str) -> Result<Option<String>, ()>,
    ) -> Result<Self, ConfigurationError> {
        let database_url = required_environment_value(&mut read, DATABASE_URL_ENV)?;
        let development = optional_environment_bool(&mut read, DEV_MODE_ENV)?.unwrap_or(false);
        let configured_auto_migrate = optional_environment_bool(&mut read, AUTO_MIGRATE_ENV)?;
        let migration_database_url = read(MIGRATION_DATABASE_URL_ENV)
            .map_err(|()| ConfigurationError::InvalidEnvironmentVariable {
                name: MIGRATION_DATABASE_URL_ENV,
            })?
            .filter(|value| !value.is_empty());

        let mut builder = Self::builder(database_url);
        if development {
            builder = builder.with_development_defaults();
        }
        if let Some(auto_migrate) = configured_auto_migrate {
            builder = builder.with_auto_migrate(auto_migrate);
        }
        if let Some(migration_database_url) = migration_database_url {
            builder = builder.with_migration_database_url(migration_database_url);
        }
        builder.build()
    }

    /// Returns the validated pool, timeout, transport, and lease options.
    #[must_use]
    pub const fn options(&self) -> &PostgresStoreOptions {
        &self.options
    }

    /// Returns whether startup will run the embedded migrations before the
    /// runtime pool connects.
    #[must_use]
    pub const fn auto_migrate(&self) -> bool {
        self.auto_migrate
    }

    /// Returns the selected security profile.
    #[must_use]
    pub const fn profile(&self) -> PostgresConfigurationProfile {
        self.profile
    }

    pub(crate) fn into_parts(self) -> (Box<str>, Option<Box<str>>, PostgresStoreOptions, bool) {
        (
            self.runtime_database_url,
            self.migration_database_url,
            self.options,
            self.auto_migrate,
        )
    }
}

impl fmt::Debug for PostgresStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStoreConfig")
            .field("runtime_database_url", &"[REDACTED]")
            .field(
                "migration_database_url",
                &self.migration_database_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field("options", &self.options)
            .field("auto_migrate", &self.auto_migrate)
            .field("profile", &self.profile)
            .finish()
    }
}

/// Builder for [`PostgresStoreConfig`].
pub struct PostgresStoreConfigBuilder {
    runtime_database_url: String,
    migration_database_url: Option<String>,
    options: PostgresStoreOptions,
    auto_migrate: bool,
    profile: PostgresConfigurationProfile,
}

impl PostgresStoreConfigBuilder {
    fn new(database_url: impl Into<String>) -> Self {
        Self {
            runtime_database_url: database_url.into(),
            migration_database_url: None,
            options: PostgresStoreOptions::default(),
            auto_migrate: false,
            profile: PostgresConfigurationProfile::Production,
        }
    }

    /// Replaces the bounded store options.
    #[must_use]
    pub fn with_options(mut self, options: PostgresStoreOptions) -> Self {
        self.options = options;
        self
    }

    /// Supplies the deployment-authorized migration URL.
    #[must_use]
    pub fn with_migration_database_url(mut self, database_url: impl Into<String>) -> Self {
        self.migration_database_url = Some(database_url.into());
        self
    }

    /// Enables or disables migration before the runtime pool connects.
    #[must_use]
    pub const fn with_auto_migrate(mut self, enabled: bool) -> Self {
        self.auto_migrate = enabled;
        self
    }

    /// Explicitly selects local-development defaults.
    ///
    /// The same connection URL is used for migrations unless a separate one is
    /// subsequently supplied. Call [`Self::with_options`] afterwards to retain
    /// the development profile while overriding individual transport/pool
    /// decisions.
    #[must_use]
    pub fn with_development_defaults(mut self) -> Self {
        self.profile = PostgresConfigurationProfile::Development;
        self.options = PostgresStoreOptions::default()
            .with_transport_security(PostgresTransportSecurity::Disabled)
            .with_pool_size(1, 4);
        self.auto_migrate = true;
        self
    }

    /// Validates and freezes the complete startup configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigurationError`] for malformed URLs/options or when
    /// production auto-migration lacks a distinct migration credential.
    pub fn build(self) -> Result<PostgresStoreConfig, ConfigurationError> {
        if self.runtime_database_url.is_empty() {
            return Err(ConfigurationError::EmptyDatabaseUrl);
        }
        self.options.connect_options(&self.runtime_database_url)?;

        let migration_database_url = match self.migration_database_url {
            Some(value) if value.is_empty() => return Err(ConfigurationError::EmptyDatabaseUrl),
            Some(value) => {
                self.options.connect_options(&value)?;
                Some(value)
            }
            None => None,
        };

        let migration_database_url = if self.auto_migrate {
            match (self.profile, migration_database_url) {
                (PostgresConfigurationProfile::Development, None) => {
                    Some(self.runtime_database_url.clone())
                }
                (PostgresConfigurationProfile::Production, None) => {
                    return Err(ConfigurationError::MigrationDatabaseUrlRequired);
                }
                (PostgresConfigurationProfile::Production, Some(value))
                    if value == self.runtime_database_url =>
                {
                    return Err(ConfigurationError::SharedProductionMigrationCredential);
                }
                (_, value) => value,
            }
        } else {
            migration_database_url
        };

        Ok(PostgresStoreConfig {
            runtime_database_url: self.runtime_database_url.into_boxed_str(),
            migration_database_url: migration_database_url.map(String::into_boxed_str),
            options: self.options,
            auto_migrate: self.auto_migrate,
            profile: self.profile,
        })
    }
}

impl fmt::Debug for PostgresStoreConfigBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStoreConfigBuilder")
            .field("runtime_database_url", &"[REDACTED]")
            .field(
                "migration_database_url",
                &self.migration_database_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field("options", &self.options)
            .field("auto_migrate", &self.auto_migrate)
            .field("profile", &self.profile)
            .finish()
    }
}

fn required_environment_value(
    read: &mut impl FnMut(&str) -> Result<Option<String>, ()>,
    name: &'static str,
) -> Result<String, ConfigurationError> {
    match read(name) {
        Ok(Some(value)) if !value.is_empty() => Ok(value),
        Ok(Some(_)) => Err(ConfigurationError::EmptyDatabaseUrl),
        Ok(None) => Err(ConfigurationError::MissingEnvironmentVariable { name }),
        Err(()) => Err(ConfigurationError::InvalidEnvironmentVariable { name }),
    }
}

fn optional_environment_bool(
    read: &mut impl FnMut(&str) -> Result<Option<String>, ()>,
    name: &'static str,
) -> Result<Option<bool>, ConfigurationError> {
    let Some(value) =
        read(name).map_err(|()| ConfigurationError::InvalidEnvironmentVariable { name })?
    else {
        return Ok(None);
    };
    if value.eq_ignore_ascii_case("true") {
        Ok(Some(true))
    } else if value.eq_ignore_ascii_case("false") {
        Ok(Some(false))
    } else {
        Err(ConfigurationError::InvalidEnvironmentVariable { name })
    }
}

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
    pub(crate) outbox_attempt_lease_duration: Duration,
}

impl PostgresStoreOptions {
    /// Returns the configured transport policy.
    #[must_use]
    pub const fn transport_security(&self) -> PostgresTransportSecurity {
        self.transport_security
    }

    /// Returns the duration assigned to a newly acquired worker lease.
    #[must_use]
    pub const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    /// Returns the maximum expiry horizon accepted by lease renewal.
    #[must_use]
    pub const fn maximum_lease_horizon(&self) -> Duration {
        self.maximum_lease_horizon
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

    /// Sets the fixed, non-renewable lease for one outbox network attempt.
    ///
    /// Validation rejects zero, sub-microsecond, or greater-than-five-minute
    /// values when the store connects. Adapter request timeouts must be lower
    /// than this lease.
    #[must_use]
    pub const fn with_outbox_attempt_lease(mut self, lease_duration: Duration) -> Self {
        self.outbox_attempt_lease_duration = lease_duration;
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
            (
                "outbox attempt lease duration",
                self.outbox_attempt_lease_duration,
            ),
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
        validate_lease_timing(
            "outbox attempt lease duration",
            self.outbox_attempt_lease_duration,
        )?;
        if self.lease_duration > self.maximum_lease_horizon {
            return Err(ConfigurationError::LeaseDurationExceedsMaximumHorizon);
        }
        if self.outbox_attempt_lease_duration > Duration::from_secs(5 * 60) {
            return Err(ConfigurationError::OutboxAttemptLeaseTooLong);
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
            outbox_attempt_lease_duration: Duration::from_secs(60),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const RUNTIME_URL: &str = "postgres://runtime:secret@db.example/stateknot";
    const MIGRATION_URL: &str = "postgres://migrator:secret@db.example/stateknot";

    fn from_values(
        values: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> Result<PostgresStoreConfig, ConfigurationError> {
        let values = values
            .into_iter()
            .map(|(name, value)| (name, value.to_owned()))
            .collect::<BTreeMap<_, _>>();
        PostgresStoreConfig::from_environment(|name| Ok(values.get(name).cloned()))
    }

    #[test]
    fn production_builder_defaults_are_safe_and_urls_are_redacted() {
        let config = PostgresStoreConfig::builder(RUNTIME_URL).build().unwrap();

        assert_eq!(config.profile(), PostgresConfigurationProfile::Production);
        assert!(!config.auto_migrate());
        assert_eq!(
            config.options().transport_security(),
            PostgresTransportSecurity::VerifyFull
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains(RUNTIME_URL));
        assert!(!debug.contains("db.example"));
        assert!(!debug.contains("secret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn development_profile_is_explicit_bounded_and_self_migrating() {
        let config = PostgresStoreConfig::development(RUNTIME_URL).unwrap();
        let (runtime, migration, options, auto_migrate) = config.into_parts();

        assert_eq!(runtime.as_ref(), RUNTIME_URL);
        assert_eq!(migration.as_deref(), Some(RUNTIME_URL));
        assert_eq!(
            options.transport_security(),
            PostgresTransportSecurity::Disabled
        );
        assert_eq!(options.min_connections, 1);
        assert_eq!(options.max_connections, 4);
        assert!(auto_migrate);
    }

    #[test]
    fn production_auto_migration_requires_distinct_credentials() {
        assert_eq!(
            PostgresStoreConfig::builder(RUNTIME_URL)
                .with_auto_migrate(true)
                .build()
                .unwrap_err(),
            ConfigurationError::MigrationDatabaseUrlRequired
        );
        assert_eq!(
            PostgresStoreConfig::builder(RUNTIME_URL)
                .with_auto_migrate(true)
                .with_migration_database_url(RUNTIME_URL)
                .build()
                .unwrap_err(),
            ConfigurationError::SharedProductionMigrationCredential
        );

        let config = PostgresStoreConfig::builder(RUNTIME_URL)
            .with_auto_migrate(true)
            .with_migration_database_url(MIGRATION_URL)
            .build()
            .unwrap();
        assert!(config.auto_migrate());
    }

    #[test]
    fn environment_profiles_are_closed_and_deterministic() {
        let development =
            from_values([(DATABASE_URL_ENV, RUNTIME_URL), (DEV_MODE_ENV, "TrUe")]).unwrap();
        assert_eq!(
            development.profile(),
            PostgresConfigurationProfile::Development
        );
        assert!(development.auto_migrate());

        let production = from_values([
            (DATABASE_URL_ENV, RUNTIME_URL),
            (AUTO_MIGRATE_ENV, "true"),
            (MIGRATION_DATABASE_URL_ENV, MIGRATION_URL),
        ])
        .unwrap();
        assert_eq!(
            production.profile(),
            PostgresConfigurationProfile::Production
        );
        assert!(production.auto_migrate());

        assert_eq!(
            from_values([(DATABASE_URL_ENV, RUNTIME_URL), (DEV_MODE_ENV, "yes")]).unwrap_err(),
            ConfigurationError::InvalidEnvironmentVariable { name: DEV_MODE_ENV }
        );
        assert_eq!(
            from_values([]).unwrap_err(),
            ConfigurationError::MissingEnvironmentVariable {
                name: DATABASE_URL_ENV
            }
        );
    }
}
