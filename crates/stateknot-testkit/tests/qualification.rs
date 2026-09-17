// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Adversarial and property qualification-evidence tests.

use proptest::prelude::*;
use serde_json::Value;
use stateknot_testkit::*;
use std::{thread::sleep, time::Duration};

const GIB: u64 = 1_073_741_824;

fn environment() -> QualificationEnvironment {
    QualificationEnvironment::new(
        SourceIdentity::new(
            "1".repeat(40),
            "2".repeat(40),
            "3".repeat(64),
            "4".repeat(64),
            "5".repeat(64),
        )
        .unwrap(),
        MachineEnvironment::new(2, 4 * GIB, "test-cpu", "local-ssd", "linux-test", "none").unwrap(),
        MachineEnvironment::new(2, 4 * GIB, "test-cpu", "local-ssd", "linux-test", "none").unwrap(),
        PostgresEnvironment::new(17, false, StandbyMode::None).unwrap(),
        Topology::new(1, 1, 1, 1).unwrap(),
        1_000,
    )
    .unwrap()
}

fn report() -> QualificationReport {
    let window = QualificationWindow::new(
        Duration::from_millis(1),
        Duration::from_millis(1),
        Duration::from_millis(1),
    )
    .unwrap();
    let plan = FaultPlan::ci_default();
    let run = QualificationRun::start(
        QualificationProfile::CiReduced,
        QualificationScenario::AgentHostControlPlaneV1,
        window,
        environment(),
        &plan,
    )
    .unwrap();
    sleep(Duration::from_millis(2));
    run.begin_measurement().unwrap();
    let recorder = run.recorder();
    for signal in LatencySignal::ALL {
        recorder
            .record_latency(
                signal,
                Duration::from_micros(1_000),
                Some(Duration::from_micros(100)),
            )
            .unwrap();
    }
    let operation = recorder.begin_operation().unwrap();
    operation.finish(OperationOutcome::Completed).unwrap();
    recorder.record_saturation_basis_points(2_000).unwrap();
    recorder
        .record_fairness(FairnessEvidence::new(100, 150, 1_000).unwrap())
        .unwrap();
    for requirement in plan.requirements() {
        recorder.fault_injected(requirement.case()).unwrap();
        recorder.fault_recovered(requirement.case()).unwrap();
        for _ in 0..requirement.required_invariant_checks() {
            recorder
                .fault_invariant_checked(requirement.case())
                .unwrap();
        }
    }
    sleep(Duration::from_millis(2));
    run.begin_drain().unwrap();
    sleep(Duration::from_millis(2));
    run.finish().unwrap()
}

#[test]
fn corrected_histograms_and_integrity_are_non_vacuous_and_deterministic() {
    let report = report();
    assert_eq!(report.counters().offered, 1);
    assert!(
        report
            .latencies()
            .iter()
            .all(|latency| latency.observed_samples == 1
                && latency.histogram_samples > latency.observed_samples
                && !latency.buckets.is_empty())
    );
    let envelope = report.to_integrity_envelope().unwrap();
    let first = envelope.canonical_bytes().unwrap();
    let second = envelope.canonical_bytes().unwrap();
    assert_eq!(first, second);
    assert_eq!(IntegrityEnvelope::from_json(&first).unwrap(), envelope);
}

#[test]
fn tampering_unknown_fields_and_oversized_inputs_fail_closed() {
    let bytes = report()
        .to_integrity_envelope()
        .unwrap()
        .canonical_bytes()
        .unwrap();
    let mut value: Value = serde_json::from_slice(&bytes).unwrap();
    value["report"]["counters"]["lost_acknowledged_records"] = Value::from(1);
    assert!(IntegrityEnvelope::from_json(&serde_json::to_vec(&value).unwrap()).is_err());

    let mut value: Value = serde_json::from_slice(&bytes).unwrap();
    value["unexpected"] = Value::Bool(true);
    assert_eq!(
        IntegrityEnvelope::from_json(&serde_json::to_vec(&value).unwrap()),
        Err(ReportError::InvalidJson)
    );
    assert_eq!(
        IntegrityEnvelope::from_json(&vec![b' '; 8 * 1_024 * 1_024 + 1]),
        Err(ReportError::EnvelopeTooLarge)
    );
}

#[test]
fn phase_and_fault_transitions_fail_closed() {
    let plan = FaultPlan::ci_default();
    let run = QualificationRun::start(
        QualificationProfile::CiReduced,
        QualificationScenario::AgentHostControlPlaneV1,
        QualificationWindow::new(
            Duration::from_millis(100),
            Duration::from_millis(1),
            Duration::from_millis(1),
        )
        .unwrap(),
        environment(),
        &plan,
    )
    .unwrap();
    assert_eq!(run.begin_measurement(), Err(RunError::PhaseTooShort));
    assert!(matches!(
        run.recorder().fault_injected(FaultCaseId::WorkerLoss),
        Err(RunError::WrongPhase { .. })
    ));
    sleep(Duration::from_millis(101));
    run.begin_measurement().unwrap();
    assert!(matches!(
        run.recorder().fault_injected(FaultCaseId::WorkerLoss),
        Err(RunError::Fault(FaultError::UnplannedCase(
            FaultCaseId::WorkerLoss
        )))
    ));
}

proptest! {
    #[test]
    fn source_identity_accepts_only_lowercase_fixed_width_hex(
        candidate in prop::collection::vec(any::<u8>(), 0..100)
    ) {
        let candidate = String::from_utf8_lossy(&candidate).into_owned();
        let valid = candidate.len() == 40
            && candidate.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        let result = SourceIdentity::new(
            &candidate,
            "a".repeat(40),
            "b".repeat(64),
            "c".repeat(64),
            "d".repeat(64),
        );
        prop_assert_eq!(result.is_ok(), valid);
    }
}
