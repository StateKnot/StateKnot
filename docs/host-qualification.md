<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Host capacity and recovery qualification

`stateknot-testkit` is a public-alpha, runtime-neutral evidence component for
StateKnot host qualification. It owns measurement timing, bounds every recorded
value, evaluates a closed objective set and emits deterministic JSON protected by
a SHA-256 integrity digest. It does not provision infrastructure, inject faults,
sign provenance or turn a CI smoke run into a production service-level claim.

This guide implements [RFC-0017](rfcs/0017-host-qualification-harness.md), which
remains Draft. The crate and report schema are pre-alpha.

## Profiles are not interchangeable

| Profile | Purpose | Release claim |
|---|---|---|
| `CiReduced` | Correctness, recovery wiring and report validation on bounded CI resources | Always `release_qualified: false` |
| `ReleaseCandidate` | Full reference topology, load, timing and fault matrix | Possible only when every mandatory objective passes |

The release profile refuses to start unless it receives the complete version-one
fault plan, PostgreSQL 16 or 17 with synchronous commit and standby, the minimum
documented topology and resources, and at most 2 ms median database RTT. It also
requires at least 10 minutes of warm-up, 30 minutes of measurement and 5 minutes
of drain. These windows use recorder-owned monotonic time; a driver cannot submit
fabricated elapsed durations.

## Record one bounded run

```rust,ignore
let plan = FaultPlan::ci_default();
let run = QualificationRun::start(
    QualificationProfile::CiReduced,
    QualificationScenario::AgentHostControlPlaneV1,
    QualificationWindow::ci_default(),
    environment,
    &plan,
)?;

// Warm the exact target, then let the recorder validate elapsed monotonic time.
run.begin_measurement()?;
let recorder = run.recorder();
let operation = recorder.begin_operation()?;

recorder.record_latency(
    LatencySignal::AdmissionCommit,
    admission_elapsed,
    Some(expected_offer_interval),
)?;
operation.finish(OperationOutcome::Completed)?;

recorder.fault_injected(FaultCaseId::HostRollingReplacement)?;
// Perform the externally controlled replacement and check declared invariants.
recorder.fault_invariant_checked(FaultCaseId::HostRollingReplacement)?;
recorder.fault_recovered(FaultCaseId::HostRollingReplacement)?;

run.begin_drain()?;
let report = run.finish()?;
let bytes = report.to_integrity_envelope()?.canonical_bytes()?;
```

An operation guard increments the in-flight count. Dropping a measured guard
without an outcome records an unexpected failure; drain cannot finish while any
guard remains. The one-million-operation in-flight ceiling rejects explicitly
instead of creating a hidden harness queue.

## What the report proves

Every run binds the Git commit/tree, `Cargo.lock`, dataset and configuration
SHA-256 values, machine/database/topology facts and measured database RTT. Four
shared signals use HDR histograms from 1 microsecond through 1 hour with three
significant digits: admission commit, runnable-to-claim, connected SSE delivery
and SSE reconnect. A fixed-rate driver supplies its expected offer interval so
the histogram can correct coordinated omission while retaining both observed
and stored sample counts.

Checked counters cover offered, accepted, completed and rejected operations,
harness rejections, expected injected failures, unexpected failures, lost
acknowledged records, accepted stale writes, cross-tenant disclosures and
duplicate external effects. Correctness/security counters, unexpected failures
and incomplete planned faults fail every profile. Latency, saturation and
noisy-neighbour fairness thresholds are diagnostic in `CiReduced` and mandatory
only in a valid release profile.

Unmeasured release-only saturation or fairness is serialized as `null` with an
`informational_missing` objective in the reduced profile. It is never converted
to zero or a pass. The release profile treats the same absence as failure.

## Fault matrix

Version one has stable cases for database unavailability, host dependency
readiness loss, Worker loss, rolling host replacement, verifier unavailability
and operations-policy expiry. Each planned case records injection, recovery and
a bounded required invariant count. Free-form fault names and unplanned updates
are rejected.

The reduced PostgreSQL 16/17 CI driver intentionally plans only dependency loss
and rolling replacement. It exercises real HTTP admission through terminal Graph
execution, exact journal timestamps for runnable-to-claim, live and replayed SSE,
sanitized protected operations, readiness refusal/recovery, replacement-before-
drain and post-replacement execution. It emits exactly one verified
`STATEKNOT_HOST_QUALIFICATION_REPORT` marker. The timing values are diagnostics
for that CI runner, not capacity numbers for another deployment.

## Integrity is not provenance

`IntegrityEnvelope` canonicalizes the strict report and recomputes its SHA-256 on
read. Unknown fields, unsupported versions, invalid bounds, inconsistent derived
objectives and tampering fail closed. Anyone can recompute a SHA-256, so the
envelope does not identify who ran the test. A release decision must additionally
bind the artifact to reviewed source and a trusted execution/attestation system.

The full production gate still requires the documented reference load, complete
fault matrix, real identity/policy deployment, 24-hour soak, database and object
store failover, backup restore, N-1/N-2 upgrade coverage and security review.
