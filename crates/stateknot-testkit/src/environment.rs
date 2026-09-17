// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_LABEL_BYTES: usize = 128;
const MAX_DATABASE_RTT_MICROS: u64 = 60_000_000;
const GIB: u64 = 1_073_741_824;

/// Invalid or incomplete qualification-environment metadata.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EnvironmentError {
    /// A fixed-width hexadecimal identity is invalid.
    #[error("{field} must be {width} lowercase hexadecimal characters")]
    InvalidHex {
        /// Field name.
        field: &'static str,
        /// Required width.
        width: usize,
    },
    /// A label is empty, oversized or contains a control character.
    #[error("{0} is not a bounded printable label")]
    InvalidLabel(&'static str),
    /// A numeric field is outside its supported bound.
    #[error("{0} is outside its supported bound")]
    InvalidNumber(&'static str),
}

/// Immutable source and input identities for one qualification run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIdentity {
    source_commit: String,
    source_tree: String,
    cargo_lock_sha256: String,
    dataset_sha256: String,
    configuration_sha256: String,
}

impl SourceIdentity {
    /// Creates and validates the source identity.
    pub fn new(
        source_commit: impl Into<String>,
        source_tree: impl Into<String>,
        cargo_lock_sha256: impl Into<String>,
        dataset_sha256: impl Into<String>,
        configuration_sha256: impl Into<String>,
    ) -> Result<Self, EnvironmentError> {
        let value = Self {
            source_commit: source_commit.into(),
            source_tree: source_tree.into(),
            cargo_lock_sha256: cargo_lock_sha256.into(),
            dataset_sha256: dataset_sha256.into(),
            configuration_sha256: configuration_sha256.into(),
        };
        value.validate()?;
        Ok(value)
    }

    /// Returns the exact Git commit.
    pub fn source_commit(&self) -> &str {
        &self.source_commit
    }

    /// Returns the exact Git tree.
    pub fn source_tree(&self) -> &str {
        &self.source_tree
    }

    pub(crate) fn validate(&self) -> Result<(), EnvironmentError> {
        valid_hex("source_commit", &self.source_commit, 40)?;
        valid_hex("source_tree", &self.source_tree, 40)?;
        valid_hex("cargo_lock_sha256", &self.cargo_lock_sha256, 64)?;
        valid_hex("dataset_sha256", &self.dataset_sha256, 64)?;
        valid_hex("configuration_sha256", &self.configuration_sha256, 64)
    }
}

/// Recorded resources for each application or database node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineEnvironment {
    logical_cpus: u16,
    memory_bytes: u64,
    cpu_model: String,
    storage_class: String,
    kernel: String,
    container_runtime: String,
}

impl MachineEnvironment {
    /// Creates bounded machine metadata.
    pub fn new(
        logical_cpus: u16,
        memory_bytes: u64,
        cpu_model: impl Into<String>,
        storage_class: impl Into<String>,
        kernel: impl Into<String>,
        container_runtime: impl Into<String>,
    ) -> Result<Self, EnvironmentError> {
        let value = Self {
            logical_cpus,
            memory_bytes,
            cpu_model: cpu_model.into(),
            storage_class: storage_class.into(),
            kernel: kernel.into(),
            container_runtime: container_runtime.into(),
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), EnvironmentError> {
        if self.logical_cpus == 0 || self.logical_cpus > 1_024 {
            return Err(EnvironmentError::InvalidNumber("logical_cpus"));
        }
        if self.memory_bytes == 0 || self.memory_bytes > 16 * 1_024 * GIB {
            return Err(EnvironmentError::InvalidNumber("memory_bytes"));
        }
        valid_label("cpu_model", &self.cpu_model)?;
        valid_label("storage_class", &self.storage_class)?;
        valid_label("kernel", &self.kernel)?;
        valid_label("container_runtime", &self.container_runtime)
    }
}

/// Database standby mode recorded by a qualification run.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StandbyMode {
    /// No standby was configured.
    None,
    /// A standby existed without synchronous acknowledgement.
    Asynchronous,
    /// At least one synchronous standby acknowledged commits.
    Synchronous,
}

/// `PostgreSQL` configuration relevant to the qualification contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresEnvironment {
    major_version: u16,
    synchronous_commit: bool,
    standby_mode: StandbyMode,
}

impl PostgresEnvironment {
    /// Creates bounded `PostgreSQL` metadata.
    pub fn new(
        major_version: u16,
        synchronous_commit: bool,
        standby_mode: StandbyMode,
    ) -> Result<Self, EnvironmentError> {
        let value = Self {
            major_version,
            synchronous_commit,
            standby_mode,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), EnvironmentError> {
        if !(12..=99).contains(&self.major_version) {
            return Err(EnvironmentError::InvalidNumber("postgres_major_version"));
        }
        Ok(())
    }
}

/// Process topology used by the Agent host qualification scenario.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    application_nodes: u16,
    api_processes: u16,
    scheduler_processes: u16,
    worker_processes: u16,
}

impl Topology {
    /// Creates a bounded topology description.
    pub fn new(
        application_nodes: u16,
        api_processes: u16,
        scheduler_processes: u16,
        worker_processes: u16,
    ) -> Result<Self, EnvironmentError> {
        let value = Self {
            application_nodes,
            api_processes,
            scheduler_processes,
            worker_processes,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), EnvironmentError> {
        for (name, value) in [
            ("application_nodes", self.application_nodes),
            ("api_processes", self.api_processes),
            ("scheduler_processes", self.scheduler_processes),
            ("worker_processes", self.worker_processes),
        ] {
            if value == 0 || value > 4_096 {
                return Err(EnvironmentError::InvalidNumber(name));
            }
        }
        Ok(())
    }
}

/// Complete bounded environment record embedded in qualification evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationEnvironment {
    source: SourceIdentity,
    application_machine: MachineEnvironment,
    database_machine: MachineEnvironment,
    postgres: PostgresEnvironment,
    topology: Topology,
    median_database_rtt_micros: u64,
}

impl QualificationEnvironment {
    /// Creates a qualification environment after validating every component.
    pub fn new(
        source: SourceIdentity,
        application_machine: MachineEnvironment,
        database_machine: MachineEnvironment,
        postgres: PostgresEnvironment,
        topology: Topology,
        median_database_rtt_micros: u64,
    ) -> Result<Self, EnvironmentError> {
        let value = Self {
            source,
            application_machine,
            database_machine,
            postgres,
            topology,
            median_database_rtt_micros,
        };
        value.validate()?;
        Ok(value)
    }

    /// Returns the exact source identity.
    pub fn source(&self) -> &SourceIdentity {
        &self.source
    }

    /// Validates internal bounds after construction or deserialization.
    pub fn validate(&self) -> Result<(), EnvironmentError> {
        self.source.validate()?;
        self.application_machine.validate()?;
        self.database_machine.validate()?;
        self.postgres.validate()?;
        self.topology.validate()?;
        if self.median_database_rtt_micros == 0
            || self.median_database_rtt_micros > MAX_DATABASE_RTT_MICROS
        {
            return Err(EnvironmentError::InvalidNumber(
                "median_database_rtt_micros",
            ));
        }
        Ok(())
    }

    /// Returns every reason this environment is not the documented reference profile.
    pub fn reference_violations(&self) -> Vec<ReferenceEnvironmentViolation> {
        let mut violations = Vec::new();
        if !matches!(self.postgres.major_version, 16 | 17) {
            violations.push(ReferenceEnvironmentViolation::PostgresVersion);
        }
        if !self.postgres.synchronous_commit {
            violations.push(ReferenceEnvironmentViolation::SynchronousCommit);
        }
        if self.postgres.standby_mode != StandbyMode::Synchronous {
            violations.push(ReferenceEnvironmentViolation::SynchronousStandby);
        }
        if self.application_machine.logical_cpus < 8
            || self.application_machine.memory_bytes < 16 * GIB
        {
            violations.push(ReferenceEnvironmentViolation::ApplicationResources);
        }
        if self.database_machine.logical_cpus < 8 || self.database_machine.memory_bytes < 32 * GIB {
            violations.push(ReferenceEnvironmentViolation::DatabaseResources);
        }
        if self.topology.application_nodes < 3
            || self.topology.api_processes < 3
            || self.topology.scheduler_processes < 3
            || self.topology.worker_processes < 6
        {
            violations.push(ReferenceEnvironmentViolation::Topology);
        }
        if self.median_database_rtt_micros > 2_000 {
            violations.push(ReferenceEnvironmentViolation::DatabaseRtt);
        }
        violations
    }
}

/// Stable reason that an environment does not satisfy the reference profile.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceEnvironmentViolation {
    /// `PostgreSQL` is not version 16 or 17.
    PostgresVersion,
    /// Synchronous commit is disabled.
    SynchronousCommit,
    /// No synchronous standby is present.
    SynchronousStandby,
    /// Application nodes are smaller than 8 vCPU and 16 GiB.
    ApplicationResources,
    /// The database node is smaller than 8 vCPU and 32 GiB.
    DatabaseResources,
    /// The role topology is below the reference counts.
    Topology,
    /// Median application-to-database RTT exceeds 2 ms.
    DatabaseRtt,
}

fn valid_hex(field: &'static str, value: &str, width: usize) -> Result<(), EnvironmentError> {
    if value.len() != width
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(EnvironmentError::InvalidHex { field, width });
    }
    Ok(())
}

fn valid_label(field: &'static str, value: &str) -> Result<(), EnvironmentError> {
    if value.is_empty() || value.len() > MAX_LABEL_BYTES || value.chars().any(char::is_control) {
        return Err(EnvironmentError::InvalidLabel(field));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_environment(reference: bool) -> QualificationEnvironment {
    let hex40 = "a".repeat(40);
    let hex64 = "b".repeat(64);
    QualificationEnvironment::new(
        SourceIdentity::new(&hex40, &hex40, &hex64, &hex64, &hex64).unwrap(),
        MachineEnvironment::new(
            if reference { 8 } else { 2 },
            16 * GIB,
            "test-cpu",
            "nvme",
            "linux",
            "none",
        )
        .unwrap(),
        MachineEnvironment::new(8, 32 * GIB, "test-cpu", "nvme", "linux", "none").unwrap(),
        PostgresEnvironment::new(
            17,
            reference,
            if reference {
                StandbyMode::Synchronous
            } else {
                StandbyMode::None
            },
        )
        .unwrap(),
        Topology::new(if reference { 3 } else { 1 }, 3, 3, 6).unwrap(),
        1_000,
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_validation_is_explicit() {
        assert!(test_environment(true).reference_violations().is_empty());
        assert_eq!(
            test_environment(false).reference_violations(),
            vec![
                ReferenceEnvironmentViolation::SynchronousCommit,
                ReferenceEnvironmentViolation::SynchronousStandby,
                ReferenceEnvironmentViolation::ApplicationResources,
                ReferenceEnvironmentViolation::Topology,
            ]
        );
    }

    #[test]
    fn identities_and_labels_are_bounded() {
        assert!(matches!(
            SourceIdentity::new(
                "A".repeat(40),
                "a".repeat(40),
                "b".repeat(64),
                "b".repeat(64),
                "b".repeat(64)
            ),
            Err(EnvironmentError::InvalidHex {
                field: "source_commit",
                ..
            })
        ));
        assert!(
            MachineEnvironment::new(1, GIB, "test-cpu", "nvme\nsecret", "linux", "none").is_err()
        );
    }
}
