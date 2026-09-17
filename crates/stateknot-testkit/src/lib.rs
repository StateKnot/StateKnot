// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded, runtime-neutral qualification evidence for `StateKnot` deployments.
//!
//! This crate records measurements and evaluates closed objectives. It does not
//! inject faults, provision infrastructure or turn CI output into a production
//! service-level claim.

#![forbid(unsafe_code)]

mod environment;
mod fault;
mod report;
mod run;

pub use environment::{
    EnvironmentError, MachineEnvironment, PostgresEnvironment, QualificationEnvironment,
    ReferenceEnvironmentViolation, SourceIdentity, StandbyMode, Topology,
};
pub use fault::{FaultCaseId, FaultError, FaultPlan, FaultRequirement, FaultResult};
pub use report::{
    EvidenceLimitation, FairnessEvidence, IntegrityEnvelope, LatencyBucket, LatencyDistribution,
    LatencySignal, ObjectiveId, ObjectiveResult, ObjectiveStatus, PhaseDurations,
    QualificationCounter, QualificationCounters, QualificationProfile, QualificationReport,
    QualificationScenario, ReportError,
};
pub use run::{
    Operation, OperationOutcome, QualificationRecorder, QualificationRun, QualificationWindow,
    RunError, RunPhase,
};
