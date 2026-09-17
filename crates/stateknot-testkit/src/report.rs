// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    environment::{EnvironmentError, QualificationEnvironment, ReferenceEnvironmentViolation},
    fault::{FaultCaseId, FaultError, FaultResult},
    run::QualificationWindow,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, fmt::Write as _};
use thiserror::Error;

const REPORT_SCHEMA_VERSION: u16 = 1;
const ENVELOPE_SCHEMA_VERSION: u16 = 1;
const MAX_ENVELOPE_BYTES: usize = 8 * 1_024 * 1_024;
const MAX_BUCKETS: usize = 16_384;

/// Qualification execution profile.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationProfile {
    /// A small correctness-oriented CI or developer profile.
    CiReduced,
    /// The full reference-environment release candidate profile.
    ReleaseCandidate,
}

/// Closed qualification scenario identifier.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationScenario {
    /// Concrete Agent host control-plane admission, execution and recovery.
    AgentHostControlPlaneV1,
}

/// Shared latency signal with a version-one objective.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencySignal {
    /// Client request start through durable admission response.
    AdmissionCommit,
    /// Runnable durable work through a fenced Worker claim.
    RunnableClaim,
    /// Durable event commit through client receipt on an established SSE stream.
    SseDelivery,
    /// Reconnect request through the first replayed event.
    SseReconnect,
}

impl LatencySignal {
    /// Every shared latency signal required for valid evidence.
    pub const ALL: [Self; 4] = [
        Self::AdmissionCommit,
        Self::RunnableClaim,
        Self::SseDelivery,
        Self::SseReconnect,
    ];
}

/// One non-empty HDR histogram bucket.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyBucket {
    /// Inclusive upper value represented by this bucket, in microseconds.
    pub upper_bound_micros: u64,
    /// Observations represented since the previous recorded bucket.
    pub count: u64,
}

/// Bounded latency distribution emitted by the recorder.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyDistribution {
    /// Signal measured by this distribution.
    pub signal: LatencySignal,
    /// Calls made by the driver before coordinated-omission correction.
    pub observed_samples: u64,
    /// Samples stored in the HDR histogram after correction.
    pub histogram_samples: u64,
    /// 50th percentile in microseconds.
    pub p50_micros: u64,
    /// 95th percentile in microseconds.
    pub p95_micros: u64,
    /// 99th percentile in microseconds.
    pub p99_micros: u64,
    /// Maximum value in microseconds.
    pub max_micros: u64,
    /// Non-empty recorded histogram buckets.
    pub buckets: Vec<LatencyBucket>,
}

impl LatencyDistribution {
    pub(crate) fn validate(&self) -> Result<(), ReportError> {
        if self.observed_samples == 0
            || self.histogram_samples < self.observed_samples
            || self.buckets.is_empty()
            || self.buckets.len() > MAX_BUCKETS
            || self.buckets.iter().any(|bucket| bucket.count == 0)
            || self
                .buckets
                .windows(2)
                .any(|pair| pair[0].upper_bound_micros >= pair[1].upper_bound_micros)
            || self
                .buckets
                .iter()
                .try_fold(0_u64, |sum, bucket| sum.checked_add(bucket.count))
                != Some(self.histogram_samples)
            || !(self.p50_micros <= self.p95_micros
                && self.p95_micros <= self.p99_micros
                && self.p99_micros <= self.max_micros)
        {
            return Err(ReportError::InvalidReport("invalid latency distribution"));
        }
        Ok(())
    }
}

/// Counter selected by a driver for an exceptional correctness observation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QualificationCounter {
    /// An acknowledged durable record could not be recovered.
    LostAcknowledgedRecords,
    /// A stale-fence mutation was accepted.
    AcceptedStaleWrites,
    /// Data crossed a tenant authorization boundary.
    CrossTenantDisclosures,
    /// An externally visible effect was duplicated.
    DuplicateExternalEffects,
}

/// Exact operation and correctness counters.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationCounters {
    /// Operations offered during the measurement phase.
    pub offered: u64,
    /// Offered operations accepted by the target.
    pub accepted: u64,
    /// Accepted operations completed successfully.
    pub completed: u64,
    /// Offered operations rejected by the target or harness ceiling.
    pub rejected: u64,
    /// Rejections caused by the explicit harness in-flight ceiling.
    pub harness_capacity_rejections: u64,
    /// Accepted operations ending in an intentionally injected failure.
    pub expected_injected_failures: u64,
    /// Accepted operations ending in any unexpected failure.
    pub unexpected_failures: u64,
    /// Acknowledged records that could not be recovered.
    pub lost_acknowledged_records: u64,
    /// Stale-fence writes accepted by a durable authority.
    pub accepted_stale_writes: u64,
    /// Data disclosed across a tenant boundary.
    pub cross_tenant_disclosures: u64,
    /// Duplicate externally visible effects.
    pub duplicate_external_effects: u64,
}

impl QualificationCounters {
    pub(crate) fn checked_add(
        &mut self,
        counter: QualificationCounter,
        amount: u64,
    ) -> Result<(), ReportError> {
        let target = match counter {
            QualificationCounter::LostAcknowledgedRecords => &mut self.lost_acknowledged_records,
            QualificationCounter::AcceptedStaleWrites => &mut self.accepted_stale_writes,
            QualificationCounter::CrossTenantDisclosures => &mut self.cross_tenant_disclosures,
            QualificationCounter::DuplicateExternalEffects => &mut self.duplicate_external_effects,
        };
        *target = target
            .checked_add(amount)
            .ok_or(ReportError::CounterOverflow)?;
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), ReportError> {
        if self.accepted.checked_add(self.rejected) != Some(self.offered)
            || self.harness_capacity_rejections > self.rejected
            || self
                .completed
                .checked_add(self.expected_injected_failures)
                .and_then(|value| value.checked_add(self.unexpected_failures))
                != Some(self.accepted)
        {
            return Err(ReportError::InvalidReport(
                "inconsistent operation counters",
            ));
        }
        Ok(())
    }
}

/// Noisy-neighbour fairness evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FairnessEvidence {
    /// Uncontended in-quota p95 queue delay.
    pub uncontended_p95_queue_micros: u64,
    /// Contended in-quota p95 queue delay.
    pub contended_p95_queue_micros: u64,
    /// Longest continuously runnable-but-unclaimed interval.
    pub max_runnable_unclaimed_micros: u64,
}

impl FairnessEvidence {
    /// Creates non-zero bounded fairness evidence.
    pub fn new(
        uncontended_p95_queue_micros: u64,
        contended_p95_queue_micros: u64,
        max_runnable_unclaimed_micros: u64,
    ) -> Result<Self, ReportError> {
        let value = Self {
            uncontended_p95_queue_micros,
            contended_p95_queue_micros,
            max_runnable_unclaimed_micros,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), ReportError> {
        if self.uncontended_p95_queue_micros == 0
            || self.contended_p95_queue_micros == 0
            || self.max_runnable_unclaimed_micros == 0
            || self.uncontended_p95_queue_micros > 3_600_000_000
            || self.contended_p95_queue_micros > 3_600_000_000
            || self.max_runnable_unclaimed_micros > 3_600_000_000
        {
            return Err(ReportError::InvalidReport("invalid fairness evidence"));
        }
        Ok(())
    }
}

/// Stable objective identifier.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveId {
    /// Admission p95 is at most 150 ms.
    AdmissionP95,
    /// Admission p99 is at most 300 ms.
    AdmissionP99,
    /// Runnable-to-claim p95 is at most 500 ms.
    RunnableClaimP95,
    /// Runnable-to-claim p99 is at most 2 s.
    RunnableClaimP99,
    /// Event-to-SSE p95 is at most 250 ms.
    SseDeliveryP95,
    /// Event-to-SSE p99 is at most 1 s.
    SseDeliveryP99,
    /// SSE reconnect p95 is at most 1 s.
    SseReconnectP95,
    /// Unexpected errors remain strictly below 0.1 percent.
    UnexpectedErrorRate,
    /// Shared saturation remains strictly below 70 percent.
    Saturation,
    /// Contended in-quota queue delay remains strictly below twice baseline.
    NoisyNeighbourFairness,
    /// No runnable tenant waits continuously for more than 5 s.
    RunnableWait,
    /// No acknowledged record is lost.
    LostAcknowledgedRecords,
    /// No stale-fence write is accepted.
    AcceptedStaleWrites,
    /// No cross-tenant disclosure occurs.
    CrossTenantDisclosures,
    /// No external effect is duplicated.
    DuplicateExternalEffects,
    /// Every planned fault completes injection, recovery and invariant checks.
    FaultMatrix,
}

/// Evaluation status for one objective.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveStatus {
    /// A mandatory objective passed.
    Pass,
    /// A mandatory objective failed.
    Fail,
    /// A reduced-profile diagnostic threshold passed.
    InformationalPass,
    /// A reduced-profile diagnostic threshold failed.
    InformationalFail,
    /// A reduced profile explicitly did not collect this release-only signal.
    InformationalMissing,
}

/// Deterministic integer evaluation of one objective.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveResult {
    /// Stable objective identifier.
    pub objective: ObjectiveId,
    /// Mandatory or informational result.
    pub status: ObjectiveStatus,
    /// Observed integer value, or `None` when a reduced profile did not measure it.
    pub observed: Option<u64>,
    /// Threshold in the same unit.
    pub threshold: u64,
}

/// Monotonic elapsed time for each completed phase.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseDurations {
    /// Warm-up elapsed microseconds.
    pub warmup_micros: u64,
    /// Measurement elapsed microseconds.
    pub measurement_micros: u64,
    /// Drain elapsed microseconds.
    pub drain_micros: u64,
}

/// Explicit reason the evidence cannot establish a release qualification.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLimitation {
    /// This was intentionally a reduced CI or developer run.
    ReducedProfile,
    /// The environment does not match the documented reference profile.
    NonReferenceEnvironment,
    /// The complete version-one release fault matrix was not planned.
    IncompleteReleaseFaultMatrix,
}

/// Immutable evaluated qualification report.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationReport {
    schema_version: u16,
    profile: QualificationProfile,
    scenario: QualificationScenario,
    release_qualified: bool,
    evidence_valid: bool,
    environment: QualificationEnvironment,
    reference_environment_violations: Vec<ReferenceEnvironmentViolation>,
    window: QualificationWindow,
    phase_durations: PhaseDurations,
    started_at_unix_millis: u64,
    finished_at_unix_millis: u64,
    latencies: Vec<LatencyDistribution>,
    counters: QualificationCounters,
    max_saturation_basis_points: Option<u16>,
    fairness: Option<FairnessEvidence>,
    faults: Vec<FaultResult>,
    objectives: Vec<ObjectiveResult>,
    limitations: Vec<EvidenceLimitation>,
}

impl QualificationReport {
    /// Returns whether the evidence is internally complete and free of mandatory failures.
    pub const fn evidence_valid(&self) -> bool {
        self.evidence_valid
    }

    /// Returns whether this exact report satisfies the release profile.
    pub const fn release_qualified(&self) -> bool {
        self.release_qualified
    }

    /// Returns the exact source and environment record.
    pub const fn environment(&self) -> &QualificationEnvironment {
        &self.environment
    }

    /// Returns evaluated objectives in stable identifier order.
    pub fn objectives(&self) -> &[ObjectiveResult] {
        &self.objectives
    }

    /// Returns latency distributions in stable signal order.
    pub fn latencies(&self) -> &[LatencyDistribution] {
        &self.latencies
    }

    /// Returns exact operation and safety counters.
    pub const fn counters(&self) -> &QualificationCounters {
        &self.counters
    }

    /// Returns explicit reasons this report cannot establish release qualification.
    pub fn limitations(&self) -> &[EvidenceLimitation] {
        &self.limitations
    }

    /// Returns the executed fault matrix.
    pub fn faults(&self) -> &[FaultResult] {
        &self.faults
    }

    /// Wraps this report in a canonical SHA-256 integrity envelope.
    pub fn to_integrity_envelope(&self) -> Result<IntegrityEnvelope, ReportError> {
        IntegrityEnvelope::new(self.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build(
        profile: QualificationProfile,
        scenario: QualificationScenario,
        environment: QualificationEnvironment,
        window: QualificationWindow,
        phase_durations: PhaseDurations,
        started_at_unix_millis: u64,
        finished_at_unix_millis: u64,
        mut latencies: Vec<LatencyDistribution>,
        counters: QualificationCounters,
        max_saturation_basis_points: Option<u16>,
        fairness: Option<FairnessEvidence>,
        mut faults: Vec<FaultResult>,
        release_fault_plan_complete: bool,
    ) -> Result<Self, ReportError> {
        latencies.sort_by_key(|entry| entry.signal);
        faults.sort_by_key(FaultResult::case);
        let reference_environment_violations = environment.reference_violations();
        let objectives = evaluate_objectives(
            profile,
            &latencies,
            &counters,
            max_saturation_basis_points,
            fairness.as_ref(),
            &faults,
        )?;
        let evidence_valid = objectives
            .iter()
            .all(|result| result.status != ObjectiveStatus::Fail)
            && counters.unexpected_failures == 0;
        let mut limitations = Vec::new();
        if profile == QualificationProfile::CiReduced {
            limitations.push(EvidenceLimitation::ReducedProfile);
        }
        if !reference_environment_violations.is_empty() {
            limitations.push(EvidenceLimitation::NonReferenceEnvironment);
        }
        if !release_fault_plan_complete {
            limitations.push(EvidenceLimitation::IncompleteReleaseFaultMatrix);
        }
        let release_qualified = profile == QualificationProfile::ReleaseCandidate
            && limitations.is_empty()
            && evidence_valid
            && objectives
                .iter()
                .all(|result| result.status == ObjectiveStatus::Pass);
        let report = Self {
            schema_version: REPORT_SCHEMA_VERSION,
            profile,
            scenario,
            release_qualified,
            evidence_valid,
            environment,
            reference_environment_violations,
            window,
            phase_durations,
            started_at_unix_millis,
            finished_at_unix_millis,
            latencies,
            counters,
            max_saturation_basis_points,
            fairness,
            faults,
            objectives,
            limitations,
        };
        report.validate()?;
        Ok(report)
    }

    fn validate(&self) -> Result<(), ReportError> {
        if self.schema_version != REPORT_SCHEMA_VERSION {
            return Err(ReportError::UnsupportedReportVersion(self.schema_version));
        }
        self.environment.validate()?;
        if self.window.validate(self.profile).is_err() {
            return Err(ReportError::InvalidReport("invalid qualification window"));
        }
        self.counters.validate()?;
        if let Some(fairness) = &self.fairness {
            fairness.validate()?;
        }
        if self.started_at_unix_millis > self.finished_at_unix_millis
            || self
                .max_saturation_basis_points
                .is_some_and(|value| value > 10_000)
        {
            return Err(ReportError::InvalidReport(
                "invalid report timing or saturation",
            ));
        }
        let expected_reference = self.environment.reference_violations();
        if self.reference_environment_violations != expected_reference {
            return Err(ReportError::InvalidReport(
                "reference-environment evaluation mismatch",
            ));
        }
        let expected_micros = self.window.minimum_micros();
        if self.phase_durations.warmup_micros < expected_micros.warmup_micros
            || self.phase_durations.measurement_micros < expected_micros.measurement_micros
            || self.phase_durations.drain_micros < expected_micros.drain_micros
        {
            return Err(ReportError::InvalidReport(
                "phase shorter than declared window",
            ));
        }
        let latency_ids = self
            .latencies
            .iter()
            .map(|entry| {
                entry.validate()?;
                Ok(entry.signal)
            })
            .collect::<Result<BTreeSet<_>, ReportError>>()?;
        if self.latencies.len() != LatencySignal::ALL.len()
            || !LatencySignal::ALL.iter().all(|id| latency_ids.contains(id))
        {
            return Err(ReportError::InvalidReport(
                "missing or duplicate latency evidence",
            ));
        }
        let mut fault_ids = BTreeSet::new();
        for fault in &self.faults {
            fault.validate()?;
            if !fault_ids.insert(fault.case()) {
                return Err(ReportError::InvalidReport("duplicate fault result"));
            }
        }
        let release_fault_plan_complete = fault_ids.len() == FaultCaseId::ALL.len()
            && FaultCaseId::ALL.iter().all(|case| fault_ids.contains(case));
        let objectives = evaluate_objectives(
            self.profile,
            &self.latencies,
            &self.counters,
            self.max_saturation_basis_points,
            self.fairness.as_ref(),
            &self.faults,
        )?;
        if self.objectives != objectives {
            return Err(ReportError::InvalidReport("objective evaluation mismatch"));
        }
        let evidence_valid = objectives
            .iter()
            .all(|result| result.status != ObjectiveStatus::Fail)
            && self.counters.unexpected_failures == 0;
        let mut limitations = Vec::new();
        if self.profile == QualificationProfile::CiReduced {
            limitations.push(EvidenceLimitation::ReducedProfile);
        }
        if !expected_reference.is_empty() {
            limitations.push(EvidenceLimitation::NonReferenceEnvironment);
        }
        if !release_fault_plan_complete {
            limitations.push(EvidenceLimitation::IncompleteReleaseFaultMatrix);
        }
        let release_qualified = self.profile == QualificationProfile::ReleaseCandidate
            && limitations.is_empty()
            && evidence_valid
            && objectives
                .iter()
                .all(|result| result.status == ObjectiveStatus::Pass);
        if self.evidence_valid != evidence_valid
            || self.limitations != limitations
            || self.release_qualified != release_qualified
        {
            return Err(ReportError::InvalidReport("derived report field mismatch"));
        }
        Ok(())
    }
}

/// Canonical report plus a tamper-evident, non-authenticating digest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrityEnvelope {
    schema_version: u16,
    report: QualificationReport,
    report_sha256: String,
}

impl IntegrityEnvelope {
    fn new(report: QualificationReport) -> Result<Self, ReportError> {
        report.validate()?;
        let report_sha256 = digest(&canonical(&report)?);
        let value = Self {
            schema_version: ENVELOPE_SCHEMA_VERSION,
            report,
            report_sha256,
        };
        value.enforce_size()?;
        Ok(value)
    }

    /// Parses bounded JSON, verifies the digest and validates every derived field.
    pub fn from_json(bytes: &[u8]) -> Result<Self, ReportError> {
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(ReportError::EnvelopeTooLarge);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| ReportError::InvalidJson)?;
        value.verify()?;
        Ok(value)
    }

    /// Returns canonical JSON bytes for storage or external attestation.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ReportError> {
        self.verify()?;
        let bytes = canonical(self)?;
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(ReportError::EnvelopeTooLarge);
        }
        Ok(bytes)
    }

    /// Returns the verified immutable report.
    pub fn report(&self) -> &QualificationReport {
        &self.report
    }

    /// Recomputes integrity and all report evaluations.
    pub fn verify(&self) -> Result<(), ReportError> {
        if self.schema_version != ENVELOPE_SCHEMA_VERSION {
            return Err(ReportError::UnsupportedEnvelopeVersion(self.schema_version));
        }
        self.report.validate()?;
        if self.report_sha256 != digest(&canonical(&self.report)?) {
            return Err(ReportError::DigestMismatch);
        }
        self.enforce_size()
    }

    fn enforce_size(&self) -> Result<(), ReportError> {
        if canonical(self)?.len() > MAX_ENVELOPE_BYTES {
            return Err(ReportError::EnvelopeTooLarge);
        }
        Ok(())
    }
}

/// Qualification report construction or verification failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ReportError {
    /// Environment metadata is invalid.
    #[error(transparent)]
    Environment(#[from] EnvironmentError),
    /// Fault evidence is invalid.
    #[error(transparent)]
    Fault(#[from] FaultError),
    /// A counter overflowed.
    #[error("qualification counter overflowed")]
    CounterOverflow,
    /// The serialized envelope exceeds the hard input ceiling.
    #[error("qualification envelope exceeds the byte ceiling")]
    EnvelopeTooLarge,
    /// JSON is malformed, non-conforming or contains unknown fields.
    #[error("invalid qualification JSON")]
    InvalidJson,
    /// Canonical serialization failed.
    #[error("qualification canonicalization failed")]
    Canonicalization,
    /// The report digest does not match the canonical report.
    #[error("qualification report digest mismatch")]
    DigestMismatch,
    /// The report schema version is unsupported.
    #[error("unsupported qualification report schema version {0}")]
    UnsupportedReportVersion(u16),
    /// The envelope schema version is unsupported.
    #[error("unsupported qualification envelope schema version {0}")]
    UnsupportedEnvelopeVersion(u16),
    /// A derived or bounded report invariant is invalid.
    #[error("invalid qualification report: {0}")]
    InvalidReport(&'static str),
}

fn canonical(value: &impl Serialize) -> Result<Vec<u8>, ReportError> {
    serde_json_canonicalizer::to_vec(value).map_err(|_| ReportError::Canonicalization)
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String is infallible");
            output
        })
}

fn evaluate_objectives(
    profile: QualificationProfile,
    latencies: &[LatencyDistribution],
    counters: &QualificationCounters,
    saturation: Option<u16>,
    fairness: Option<&FairnessEvidence>,
    faults: &[FaultResult],
) -> Result<Vec<ObjectiveResult>, ReportError> {
    let mandatory_latency = profile == QualificationProfile::ReleaseCandidate;
    let mut values = Vec::with_capacity(16);
    add_latency_objectives(latencies, mandatory_latency, &mut values)?;

    let error_bps = if counters.offered == 0 {
        return Err(ReportError::InvalidReport("no offered operations"));
    } else {
        ratio_basis_points(counters.unexpected_failures, counters.offered)?
    };
    values.push(objective_result(
        ObjectiveId::UnexpectedErrorRate,
        error_bps,
        10,
        counters
            .unexpected_failures
            .checked_mul(10_000)
            .is_some_and(|left| {
                counters
                    .offered
                    .checked_mul(10)
                    .is_some_and(|right| left < right)
            }),
        true,
    ));
    if let Some(saturation) = saturation {
        values.push(objective_result(
            ObjectiveId::Saturation,
            u64::from(saturation),
            7_000,
            saturation < 7_000,
            mandatory_latency,
        ));
    } else {
        values.push(missing_objective(
            ObjectiveId::Saturation,
            7_000,
            mandatory_latency,
        ));
    }
    if let Some(fairness) = fairness {
        let fairness_bps = ratio_basis_points(
            fairness.contended_p95_queue_micros,
            fairness.uncontended_p95_queue_micros,
        )?;
        values.push(objective_result(
            ObjectiveId::NoisyNeighbourFairness,
            fairness_bps,
            20_000,
            fairness
                .uncontended_p95_queue_micros
                .checked_mul(2)
                .is_some_and(|right| fairness.contended_p95_queue_micros < right),
            mandatory_latency,
        ));
        values.push(objective_result(
            ObjectiveId::RunnableWait,
            fairness.max_runnable_unclaimed_micros,
            5_000_000,
            fairness.max_runnable_unclaimed_micros <= 5_000_000,
            mandatory_latency,
        ));
    } else {
        values.push(missing_objective(
            ObjectiveId::NoisyNeighbourFairness,
            20_000,
            mandatory_latency,
        ));
        values.push(missing_objective(
            ObjectiveId::RunnableWait,
            5_000_000,
            mandatory_latency,
        ));
    }
    add_safety_objectives(counters, &mut values);
    let incomplete = faults.iter().filter(|fault| !fault.is_complete()).count() as u64;
    values.push(objective_result(
        ObjectiveId::FaultMatrix,
        incomplete,
        0,
        incomplete == 0,
        true,
    ));
    values.sort_by_key(|result| result.objective);
    Ok(values)
}

fn add_latency_objectives(
    latencies: &[LatencyDistribution],
    mandatory: bool,
    values: &mut Vec<ObjectiveResult>,
) -> Result<(), ReportError> {
    let latency = |signal| {
        latencies
            .iter()
            .find(|entry| entry.signal == signal)
            .ok_or(ReportError::InvalidReport("missing latency evidence"))
    };
    let mut add_latency = |objective, observed, threshold| {
        values.push(objective_result(
            objective,
            observed,
            threshold,
            observed <= threshold,
            mandatory,
        ));
    };
    let admission = latency(LatencySignal::AdmissionCommit)?;
    add_latency(ObjectiveId::AdmissionP95, admission.p95_micros, 150_000);
    add_latency(ObjectiveId::AdmissionP99, admission.p99_micros, 300_000);
    let claim = latency(LatencySignal::RunnableClaim)?;
    add_latency(ObjectiveId::RunnableClaimP95, claim.p95_micros, 500_000);
    add_latency(ObjectiveId::RunnableClaimP99, claim.p99_micros, 2_000_000);
    let delivery = latency(LatencySignal::SseDelivery)?;
    add_latency(ObjectiveId::SseDeliveryP95, delivery.p95_micros, 250_000);
    add_latency(ObjectiveId::SseDeliveryP99, delivery.p99_micros, 1_000_000);
    let reconnect = latency(LatencySignal::SseReconnect)?;
    add_latency(
        ObjectiveId::SseReconnectP95,
        reconnect.p95_micros,
        1_000_000,
    );
    Ok(())
}

fn add_safety_objectives(counters: &QualificationCounters, values: &mut Vec<ObjectiveResult>) {
    for (objective, observed) in [
        (
            ObjectiveId::LostAcknowledgedRecords,
            counters.lost_acknowledged_records,
        ),
        (
            ObjectiveId::AcceptedStaleWrites,
            counters.accepted_stale_writes,
        ),
        (
            ObjectiveId::CrossTenantDisclosures,
            counters.cross_tenant_disclosures,
        ),
        (
            ObjectiveId::DuplicateExternalEffects,
            counters.duplicate_external_effects,
        ),
    ] {
        values.push(objective_result(
            objective,
            observed,
            0,
            observed == 0,
            true,
        ));
    }
}

fn objective_result(
    objective: ObjectiveId,
    observed: u64,
    threshold: u64,
    met: bool,
    mandatory: bool,
) -> ObjectiveResult {
    let status = match (mandatory, met) {
        (true, true) => ObjectiveStatus::Pass,
        (true, false) => ObjectiveStatus::Fail,
        (false, true) => ObjectiveStatus::InformationalPass,
        (false, false) => ObjectiveStatus::InformationalFail,
    };
    ObjectiveResult {
        objective,
        status,
        observed: Some(observed),
        threshold,
    }
}

fn missing_objective(objective: ObjectiveId, threshold: u64, mandatory: bool) -> ObjectiveResult {
    ObjectiveResult {
        objective,
        status: if mandatory {
            ObjectiveStatus::Fail
        } else {
            ObjectiveStatus::InformationalMissing
        },
        observed: None,
        threshold,
    }
}

fn ratio_basis_points(numerator: u64, denominator: u64) -> Result<u64, ReportError> {
    if denominator == 0 {
        return Err(ReportError::InvalidReport("zero ratio denominator"));
    }
    let scaled = u128::from(numerator) * 10_000;
    let denominator = u128::from(denominator);
    let rounded_up = scaled.div_ceil(denominator);
    u64::try_from(rounded_up).map_err(|_| ReportError::InvalidReport("ratio overflow"))
}
