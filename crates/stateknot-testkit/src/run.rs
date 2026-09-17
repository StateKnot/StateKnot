// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    environment::QualificationEnvironment,
    fault::{FaultCaseId, FaultError, FaultPlan, FaultResult},
    report::{
        FairnessEvidence, LatencyBucket, LatencyDistribution, LatencySignal, PhaseDurations,
        QualificationCounter, QualificationCounters, QualificationProfile, QualificationReport,
        QualificationScenario, ReportError,
    },
};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

const MIN_PHASE_MICROS: u64 = 1_000;
const MAX_PHASE_MICROS: u64 = 86_400_000_000;
const MAX_LATENCY_MICROS: u64 = 3_600_000_000;
const MAX_IN_FLIGHT: u64 = 1_000_000;
const MAX_BUCKETS: usize = 16_384;
const RELEASE_WARMUP_MICROS: u64 = 600_000_000;
const RELEASE_MEASUREMENT_MICROS: u64 = 1_800_000_000;
const RELEASE_DRAIN_MICROS: u64 = 300_000_000;

/// Minimum monotonic duration for each qualification phase.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)] // Units belong in the serialized field names.
pub struct QualificationWindow {
    warmup_micros: u64,
    measurement_micros: u64,
    drain_micros: u64,
}

impl QualificationWindow {
    /// Creates bounded phase windows with microsecond-exact durations.
    pub fn new(warmup: Duration, measurement: Duration, drain: Duration) -> Result<Self, RunError> {
        let value = Self {
            warmup_micros: duration_micros(warmup)?,
            measurement_micros: duration_micros(measurement)?,
            drain_micros: duration_micros(drain)?,
        };
        value.validate(QualificationProfile::CiReduced)?;
        Ok(value)
    }

    /// A practical reduced window for local or CI correctness qualification.
    pub const fn ci_default() -> Self {
        Self {
            warmup_micros: 1_000_000,
            measurement_micros: 5_000_000,
            drain_micros: 1_000_000,
        }
    }

    /// The minimum release window: 10 minute warm-up, 30 minute measurement and 5 minute drain.
    pub const fn release_default() -> Self {
        Self {
            warmup_micros: RELEASE_WARMUP_MICROS,
            measurement_micros: RELEASE_MEASUREMENT_MICROS,
            drain_micros: RELEASE_DRAIN_MICROS,
        }
    }

    /// Returns the warm-up minimum.
    pub const fn warmup(&self) -> Duration {
        Duration::from_micros(self.warmup_micros)
    }

    /// Returns the measurement minimum.
    pub const fn measurement(&self) -> Duration {
        Duration::from_micros(self.measurement_micros)
    }

    /// Returns the drain minimum.
    pub const fn drain(&self) -> Duration {
        Duration::from_micros(self.drain_micros)
    }

    pub(crate) fn validate(&self, profile: QualificationProfile) -> Result<(), RunError> {
        for value in [
            self.warmup_micros,
            self.measurement_micros,
            self.drain_micros,
        ] {
            if !(MIN_PHASE_MICROS..=MAX_PHASE_MICROS).contains(&value) {
                return Err(RunError::InvalidWindow);
            }
        }
        if profile == QualificationProfile::ReleaseCandidate
            && (self.warmup_micros < RELEASE_WARMUP_MICROS
                || self.measurement_micros < RELEASE_MEASUREMENT_MICROS
                || self.drain_micros < RELEASE_DRAIN_MICROS)
        {
            return Err(RunError::ReleaseWindowTooShort);
        }
        Ok(())
    }

    pub(crate) const fn minimum_micros(&self) -> PhaseDurations {
        PhaseDurations {
            warmup_micros: self.warmup_micros,
            measurement_micros: self.measurement_micros,
            drain_micros: self.drain_micros,
        }
    }
}

/// Monotonic qualification phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunPhase {
    /// Target warm-up; observations are deliberately not evaluated.
    Warmup,
    /// Evaluated measurement window.
    Measurement,
    /// Bounded drain; no new evaluated operations are admitted.
    Drain,
    /// Immutable report was successfully produced.
    Finished,
}

/// Final classification for an offered operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationOutcome {
    /// The target accepted and completed the operation.
    Completed,
    /// The target rejected the operation without accepting it.
    Rejected,
    /// The accepted operation ended in an intentionally injected failure.
    ExpectedInjectedFailure,
    /// The accepted operation ended in an unexpected failure.
    UnexpectedFailure,
}

/// A qualification run with recorder-owned monotonic phase transitions.
pub struct QualificationRun {
    profile: QualificationProfile,
    scenario: QualificationScenario,
    environment: QualificationEnvironment,
    window: QualificationWindow,
    release_fault_plan_complete: bool,
    started_at_unix_millis: u64,
    shared: Arc<Mutex<RunState>>,
}

impl QualificationRun {
    /// Starts in Warmup after validating profile, environment and fault plan.
    pub fn start(
        profile: QualificationProfile,
        scenario: QualificationScenario,
        window: QualificationWindow,
        environment: QualificationEnvironment,
        fault_plan: &FaultPlan,
    ) -> Result<Self, RunError> {
        environment.validate()?;
        window.validate(profile)?;
        fault_plan.validate()?;
        if profile == QualificationProfile::ReleaseCandidate {
            if !environment.reference_violations().is_empty() {
                return Err(RunError::NonReferenceReleaseEnvironment);
            }
            if !fault_plan.is_release_complete() {
                return Err(RunError::IncompleteReleaseFaultPlan);
            }
        }
        let now = Instant::now();
        let started_at_unix_millis = unix_millis(SystemTime::now())?;
        let measurements = Measurements::new()?;
        let faults = fault_plan
            .requirements()
            .iter()
            .map(FaultResult::planned)
            .collect();
        Ok(Self {
            profile,
            scenario,
            environment,
            window,
            release_fault_plan_complete: fault_plan.is_release_complete(),
            started_at_unix_millis,
            shared: Arc::new(Mutex::new(RunState {
                phase: RunPhase::Warmup,
                phase_started: now,
                warmup_micros: None,
                measurement_micros: None,
                measurements,
                faults,
            })),
        })
    }

    /// Returns a cloneable thread-safe recorder bound to this run.
    pub fn recorder(&self) -> QualificationRecorder {
        QualificationRecorder {
            shared: self.shared.clone(),
        }
    }

    /// Returns the current phase through the same synchronization boundary as recording.
    pub fn phase(&self) -> Result<RunPhase, RunError> {
        Ok(self.lock()?.phase)
    }

    /// Ends Warmup after its recorder-owned minimum and starts Measurement.
    pub fn begin_measurement(&self) -> Result<(), RunError> {
        let mut state = self.lock()?;
        require_phase(&state, RunPhase::Warmup)?;
        let elapsed = state.phase_started.elapsed();
        require_elapsed(elapsed, self.window.warmup())?;
        state.warmup_micros = Some(elapsed_micros(elapsed)?);
        state.phase = RunPhase::Measurement;
        state.phase_started = Instant::now();
        Ok(())
    }

    /// Ends Measurement after its recorder-owned minimum and starts Drain.
    pub fn begin_drain(&self) -> Result<(), RunError> {
        let mut state = self.lock()?;
        require_phase(&state, RunPhase::Measurement)?;
        let elapsed = state.phase_started.elapsed();
        require_elapsed(elapsed, self.window.measurement())?;
        state.measurement_micros = Some(elapsed_micros(elapsed)?);
        state.phase = RunPhase::Drain;
        state.phase_started = Instant::now();
        Ok(())
    }

    /// Finishes Drain with no in-flight operation and returns immutable evidence.
    pub fn finish(&self) -> Result<QualificationReport, RunError> {
        let mut state = self.lock()?;
        require_phase(&state, RunPhase::Drain)?;
        let drain = state.phase_started.elapsed();
        require_elapsed(drain, self.window.drain())?;
        if state.measurements.in_flight != 0 {
            return Err(RunError::OperationsInFlight(state.measurements.in_flight));
        }
        let saturation = state.measurements.max_saturation_basis_points;
        let fairness = state.measurements.fairness.clone();
        let latencies = state.measurements.distributions()?;
        let durations = PhaseDurations {
            warmup_micros: state
                .warmup_micros
                .ok_or(RunError::MissingEvidence("warmup duration"))?,
            measurement_micros: state
                .measurement_micros
                .ok_or(RunError::MissingEvidence("measurement duration"))?,
            drain_micros: elapsed_micros(drain)?,
        };
        let report = QualificationReport::build(
            self.profile,
            self.scenario,
            self.environment.clone(),
            self.window.clone(),
            durations,
            self.started_at_unix_millis,
            unix_millis(SystemTime::now())?,
            latencies,
            state.measurements.counters.clone(),
            saturation,
            fairness,
            state.faults.clone(),
            self.release_fault_plan_complete,
        )?;
        state.phase = RunPhase::Finished;
        Ok(report)
    }

    fn lock(&self) -> Result<MutexGuard<'_, RunState>, RunError> {
        self.shared.lock().map_err(|_| RunError::Poisoned)
    }
}

/// Thread-safe measurement handle. Every mutating method uses the phase lock.
#[derive(Clone)]
pub struct QualificationRecorder {
    shared: Arc<Mutex<RunState>>,
}

impl QualificationRecorder {
    /// Begins one offered operation. Dropping the returned guard records an unexpected failure.
    pub fn begin_operation(&self) -> Result<Operation, RunError> {
        let mut state = self.lock()?;
        if state.phase != RunPhase::Measurement {
            return Ok(Operation {
                recorder: self.clone(),
                measured: false,
                finished: false,
            });
        }
        checked_increment(&mut state.measurements.counters.offered, 1)?;
        if state.measurements.in_flight >= MAX_IN_FLIGHT {
            checked_increment(&mut state.measurements.counters.rejected, 1)?;
            checked_increment(
                &mut state.measurements.counters.harness_capacity_rejections,
                1,
            )?;
            return Err(RunError::HarnessCapacityRejected);
        }
        checked_increment(&mut state.measurements.in_flight, 1)?;
        Ok(Operation {
            recorder: self.clone(),
            measured: true,
            finished: false,
        })
    }

    /// Records a bounded latency, optionally correcting a fixed-rate interval.
    pub fn record_latency(
        &self,
        signal: LatencySignal,
        elapsed: Duration,
        expected_interval: Option<Duration>,
    ) -> Result<(), RunError> {
        let mut state = self.lock()?;
        require_phase(&state, RunPhase::Measurement)?;
        let value = latency_micros(elapsed)?;
        let expected = expected_interval.map(latency_micros).transpose()?;
        let histogram = state
            .measurements
            .latencies
            .get_mut(&signal)
            .ok_or(RunError::MissingEvidence("latency histogram"))?;
        if let Some(expected) = expected {
            histogram
                .histogram
                .record_correct(value, expected)
                .map_err(|_| RunError::LatencyOutOfRange)?;
        } else {
            histogram
                .histogram
                .record(value)
                .map_err(|_| RunError::LatencyOutOfRange)?;
        }
        checked_increment(&mut histogram.observed_samples, 1)
    }

    /// Records a correctness or security failure count during Measurement.
    pub fn record_counter(
        &self,
        counter: QualificationCounter,
        amount: u64,
    ) -> Result<(), RunError> {
        let mut state = self.lock()?;
        require_phase(&state, RunPhase::Measurement)?;
        state.measurements.counters.checked_add(counter, amount)?;
        Ok(())
    }

    /// Records current shared-bottleneck saturation and retains the maximum.
    pub fn record_saturation_basis_points(&self, value: u16) -> Result<(), RunError> {
        if value > 10_000 {
            return Err(RunError::InvalidSaturation);
        }
        let mut state = self.lock()?;
        require_phase(&state, RunPhase::Measurement)?;
        state.measurements.max_saturation_basis_points = Some(
            state
                .measurements
                .max_saturation_basis_points
                .map_or(value, |current| current.max(value)),
        );
        Ok(())
    }

    /// Records the single driver-computed fairness evidence set.
    pub fn record_fairness(&self, fairness: FairnessEvidence) -> Result<(), RunError> {
        let mut state = self.lock()?;
        require_phase(&state, RunPhase::Measurement)?;
        if state.measurements.fairness.replace(fairness).is_some() {
            return Err(RunError::DuplicateEvidence("fairness"));
        }
        Ok(())
    }

    /// Marks one planned fault injection.
    pub fn fault_injected(&self, case: FaultCaseId) -> Result<(), RunError> {
        self.update_fault(case, FaultResult::injected)
    }

    /// Marks one observed recovery for a planned fault.
    pub fn fault_recovered(&self, case: FaultCaseId) -> Result<(), RunError> {
        self.update_fault(case, FaultResult::recovered)
    }

    /// Marks one checked invariant for a planned fault.
    pub fn fault_invariant_checked(&self, case: FaultCaseId) -> Result<(), RunError> {
        self.update_fault(case, FaultResult::invariant_checked)
    }

    fn update_fault(
        &self,
        case: FaultCaseId,
        update: fn(&mut FaultResult) -> Result<(), FaultError>,
    ) -> Result<(), RunError> {
        let mut state = self.lock()?;
        require_phase(&state, RunPhase::Measurement)?;
        let result = state
            .faults
            .iter_mut()
            .find(|result| result.case() == case)
            .ok_or(FaultError::UnplannedCase(case))?;
        update(result)?;
        Ok(())
    }

    fn finish_operation(&self, outcome: OperationOutcome) -> Result<(), RunError> {
        let mut state = self.lock()?;
        if state.measurements.in_flight == 0 {
            return Err(RunError::OperationAccounting);
        }
        state.measurements.in_flight -= 1;
        match outcome {
            OperationOutcome::Completed => {
                checked_increment(&mut state.measurements.counters.accepted, 1)?;
                checked_increment(&mut state.measurements.counters.completed, 1)?;
            }
            OperationOutcome::Rejected => {
                checked_increment(&mut state.measurements.counters.rejected, 1)?;
            }
            OperationOutcome::ExpectedInjectedFailure => {
                checked_increment(&mut state.measurements.counters.accepted, 1)?;
                checked_increment(
                    &mut state.measurements.counters.expected_injected_failures,
                    1,
                )?;
            }
            OperationOutcome::UnexpectedFailure => {
                checked_increment(&mut state.measurements.counters.accepted, 1)?;
                checked_increment(&mut state.measurements.counters.unexpected_failures, 1)?;
            }
        }
        Ok(())
    }

    fn abandoned_operation(&self) {
        let Ok(mut state) = self.shared.lock() else {
            return;
        };
        if state.measurements.in_flight == 0 {
            return;
        }
        state.measurements.in_flight -= 1;
        let accepted = state.measurements.counters.accepted.checked_add(1);
        let failures = state
            .measurements
            .counters
            .unexpected_failures
            .checked_add(1);
        if let (Some(accepted), Some(failures)) = (accepted, failures) {
            state.measurements.counters.accepted = accepted;
            state.measurements.counters.unexpected_failures = failures;
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, RunState>, RunError> {
        self.shared.lock().map_err(|_| RunError::Poisoned)
    }
}

/// RAII accounting guard for one offered operation.
pub struct Operation {
    recorder: QualificationRecorder,
    measured: bool,
    finished: bool,
}

impl Operation {
    /// Returns whether this operation began during Measurement.
    pub const fn is_measured(&self) -> bool {
        self.measured
    }

    /// Completes accounting exactly once.
    pub fn finish(mut self, outcome: OperationOutcome) -> Result<(), RunError> {
        if self.measured {
            self.recorder.finish_operation(outcome)?;
        }
        self.finished = true;
        Ok(())
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        if self.measured && !self.finished {
            self.recorder.abandoned_operation();
        }
    }
}

/// Qualification recording or lifecycle failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RunError {
    /// Environment metadata is invalid.
    #[error(transparent)]
    Environment(#[from] crate::EnvironmentError),
    /// Fault plan or transition is invalid.
    #[error(transparent)]
    Fault(#[from] FaultError),
    /// Report construction failed.
    #[error(transparent)]
    Report(#[from] ReportError),
    /// A phase duration is outside the supported bound.
    #[error("qualification window is outside the supported bound")]
    InvalidWindow,
    /// The release profile is shorter than 10/30/5 minutes.
    #[error("release qualification requires at least 10/30/5 minute phases")]
    ReleaseWindowTooShort,
    /// The release profile environment is not the documented reference environment.
    #[error("release qualification requires the documented reference environment")]
    NonReferenceReleaseEnvironment,
    /// The release profile does not plan every version-one fault.
    #[error("release qualification requires the complete fault matrix")]
    IncompleteReleaseFaultPlan,
    /// The requested transition or recording call occurs in the wrong phase.
    #[error("expected qualification phase {expected:?}, found {actual:?}")]
    WrongPhase {
        /// Required phase.
        expected: RunPhase,
        /// Observed phase.
        actual: RunPhase,
    },
    /// The recorder-owned minimum has not elapsed.
    #[error("qualification phase minimum has not elapsed")]
    PhaseTooShort,
    /// An elapsed or wall-clock value cannot be represented.
    #[error("qualification clock value is not representable")]
    Clock,
    /// Shared recorder state was poisoned by a panic.
    #[error("qualification recorder state is poisoned")]
    Poisoned,
    /// A measurement exceeds the bounded HDR range.
    #[error("latency is outside the one-microsecond through one-hour range")]
    LatencyOutOfRange,
    /// A numeric counter overflowed.
    #[error("qualification counter overflowed")]
    CounterOverflow,
    /// The explicit harness capacity ceiling rejected an offered operation.
    #[error("qualification harness in-flight ceiling reached")]
    HarnessCapacityRejected,
    /// Drain cannot finish while operations remain in flight.
    #[error("{0} qualification operations remain in flight")]
    OperationsInFlight(u64),
    /// Operation accounting is internally inconsistent.
    #[error("qualification operation accounting is inconsistent")]
    OperationAccounting,
    /// Saturation is outside zero through 10,000 basis points.
    #[error("saturation basis points exceed 10,000")]
    InvalidSaturation,
    /// A single-valued evidence set was recorded more than once.
    #[error("duplicate qualification evidence: {0}")]
    DuplicateEvidence(&'static str),
    /// Required evidence was not recorded.
    #[error("missing qualification evidence: {0}")]
    MissingEvidence(&'static str),
}

struct RunState {
    phase: RunPhase,
    phase_started: Instant,
    warmup_micros: Option<u64>,
    measurement_micros: Option<u64>,
    measurements: Measurements,
    faults: Vec<FaultResult>,
}

struct Measurements {
    latencies: BTreeMap<LatencySignal, LatencyHistogram>,
    counters: QualificationCounters,
    in_flight: u64,
    max_saturation_basis_points: Option<u16>,
    fairness: Option<FairnessEvidence>,
}

impl Measurements {
    fn new() -> Result<Self, RunError> {
        let mut latencies = BTreeMap::new();
        for signal in LatencySignal::ALL {
            let histogram = Histogram::<u64>::new_with_bounds(1, MAX_LATENCY_MICROS, 3)
                .map_err(|_| RunError::LatencyOutOfRange)?;
            latencies.insert(
                signal,
                LatencyHistogram {
                    histogram,
                    observed_samples: 0,
                },
            );
        }
        Ok(Self {
            latencies,
            counters: QualificationCounters::default(),
            in_flight: 0,
            max_saturation_basis_points: None,
            fairness: None,
        })
    }

    fn distributions(&self) -> Result<Vec<LatencyDistribution>, RunError> {
        self.latencies
            .iter()
            .map(|(signal, latency)| {
                if latency.observed_samples == 0 {
                    return Err(RunError::MissingEvidence("latency samples"));
                }
                let buckets = latency
                    .histogram
                    .iter_recorded()
                    .map(|item| LatencyBucket {
                        upper_bound_micros: item.value_iterated_to(),
                        count: item.count_since_last_iteration(),
                    })
                    .collect::<Vec<_>>();
                if buckets.len() > MAX_BUCKETS {
                    return Err(RunError::Report(ReportError::InvalidReport(
                        "too many latency buckets",
                    )));
                }
                Ok(LatencyDistribution {
                    signal: *signal,
                    observed_samples: latency.observed_samples,
                    histogram_samples: latency.histogram.len(),
                    p50_micros: latency.histogram.value_at_quantile(0.50),
                    p95_micros: latency.histogram.value_at_quantile(0.95),
                    p99_micros: latency.histogram.value_at_quantile(0.99),
                    max_micros: latency.histogram.max(),
                    buckets,
                })
            })
            .collect()
    }
}

struct LatencyHistogram {
    histogram: Histogram<u64>,
    observed_samples: u64,
}

fn duration_micros(value: Duration) -> Result<u64, RunError> {
    let micros = u64::try_from(value.as_micros()).map_err(|_| RunError::InvalidWindow)?;
    if !(MIN_PHASE_MICROS..=MAX_PHASE_MICROS).contains(&micros)
        || Duration::from_micros(micros) != value
    {
        return Err(RunError::InvalidWindow);
    }
    Ok(micros)
}

fn latency_micros(value: Duration) -> Result<u64, RunError> {
    let micros =
        u64::try_from(value.as_nanos().div_ceil(1_000)).map_err(|_| RunError::LatencyOutOfRange)?;
    if !(1..=MAX_LATENCY_MICROS).contains(&micros) {
        return Err(RunError::LatencyOutOfRange);
    }
    Ok(micros)
}

fn elapsed_micros(value: Duration) -> Result<u64, RunError> {
    u64::try_from(value.as_micros()).map_err(|_| RunError::Clock)
}

fn unix_millis(value: SystemTime) -> Result<u64, RunError> {
    let elapsed = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RunError::Clock)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| RunError::Clock)
}

fn require_phase(state: &RunState, expected: RunPhase) -> Result<(), RunError> {
    if state.phase != expected {
        return Err(RunError::WrongPhase {
            expected,
            actual: state.phase,
        });
    }
    Ok(())
}

fn require_elapsed(actual: Duration, minimum: Duration) -> Result<(), RunError> {
    if actual < minimum {
        return Err(RunError::PhaseTooShort);
    }
    Ok(())
}

fn checked_increment(value: &mut u64, amount: u64) -> Result<(), RunError> {
    *value = value.checked_add(amount).ok_or(RunError::CounterOverflow)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::test_environment;
    use std::thread::sleep;

    fn small_window() -> QualificationWindow {
        QualificationWindow::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
        )
        .unwrap()
    }

    fn begin_run() -> QualificationRun {
        let run = QualificationRun::start(
            QualificationProfile::CiReduced,
            QualificationScenario::AgentHostControlPlaneV1,
            small_window(),
            test_environment(false),
            &FaultPlan::ci_default(),
        )
        .unwrap();
        sleep(Duration::from_millis(2));
        run.begin_measurement().unwrap();
        run
    }

    fn complete_evidence(run: &QualificationRun) {
        let recorder = run.recorder();
        for signal in LatencySignal::ALL {
            recorder
                .record_latency(
                    signal,
                    Duration::from_micros(100),
                    Some(Duration::from_micros(50)),
                )
                .unwrap();
        }
        recorder.record_saturation_basis_points(1_000).unwrap();
        recorder
            .record_fairness(FairnessEvidence::new(100, 150, 200).unwrap())
            .unwrap();
        for requirement in FaultPlan::ci_default().requirements() {
            recorder.fault_injected(requirement.case()).unwrap();
            recorder.fault_recovered(requirement.case()).unwrap();
            for _ in 0..requirement.required_invariant_checks() {
                recorder
                    .fault_invariant_checked(requirement.case())
                    .unwrap();
            }
        }
        recorder
            .begin_operation()
            .unwrap()
            .finish(OperationOutcome::Completed)
            .unwrap();
    }

    #[test]
    fn reduced_report_is_valid_but_never_release_qualified() {
        let run = begin_run();
        complete_evidence(&run);
        sleep(Duration::from_millis(2));
        run.begin_drain().unwrap();
        sleep(Duration::from_millis(2));
        let report = run.finish().unwrap();
        assert!(report.evidence_valid());
        assert!(!report.release_qualified());
        let bytes = report
            .to_integrity_envelope()
            .unwrap()
            .canonical_bytes()
            .unwrap();
        let parsed = crate::IntegrityEnvelope::from_json(&bytes).unwrap();
        assert_eq!(parsed.report(), &report);
    }

    #[test]
    fn abandoned_operation_is_an_explicit_failure() {
        let run = begin_run();
        complete_evidence(&run);
        drop(run.recorder().begin_operation().unwrap());
        sleep(Duration::from_millis(2));
        run.begin_drain().unwrap();
        sleep(Duration::from_millis(2));
        assert!(!run.finish().unwrap().evidence_valid());
    }

    #[test]
    fn release_profile_cannot_use_reduced_inputs() {
        assert!(matches!(
            QualificationRun::start(
                QualificationProfile::ReleaseCandidate,
                QualificationScenario::AgentHostControlPlaneV1,
                small_window(),
                test_environment(false),
                &FaultPlan::ci_default(),
            ),
            Err(RunError::ReleaseWindowTooShort)
        ));
    }
}
