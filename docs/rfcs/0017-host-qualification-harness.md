<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0017: Host qualification harness

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-09-17
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/67
- Supersedes: None
- Superseded by: None

## Summary

Add the unpublished `stateknot-testkit` crate for bounded measurement, a closed
fault matrix, objective evaluation and tamper-evident canonical reports. It makes
host capacity and recovery evidence reproducible without turning a laptop or CI
smoke result into a production service-level claim. This remains an experimental
pre-alpha contract while this RFC is Draft.

## Motivation

StateKnot has executable PostgreSQL 16/17 tests for individual correctness and
recovery boundaries, but no common record of the offered load, measurement
window, environment, latency distribution, failure counters and injected faults.
One-off benchmark output cannot establish which source or topology was tested,
whether coordinated omission hid saturation, or whether a correctness failure
was averaged away. Operators need one strict evidence format before attempting a
production qualification run.

## Goals and non-goals

Provide a bounded thread-safe recorder, recorder-owned monotonic phase timing,
HDR latency distributions, exact counters, stable objective and fault identifiers,
validated reference-environment metadata, deterministic evaluation, canonical
JSON and a SHA-256 integrity envelope. Supply a reduced real-host PostgreSQL 16/17
profile exercising admission, terminal execution, protected operations, a
dependency outage and recovery, and process replacement.

Do not provision infrastructure, manufacture reference-environment metadata,
declare a production SLO from CI, select a cloud vendor, export telemetry, sign
or attest provenance, deploy the Agent runtime, add administrative operations,
run the 24-hour soak, qualify database/object-store failover, or change durable,
HTTP, identity, lifecycle or business schemas. The integrity digest detects byte
changes; it is not an identity, signature or supply-chain attestation.

## User-facing design

The intended API, made compilable with implementation, is:

```rust,ignore
let environment = QualificationEnvironment::builder()
    .source_commit(commit)?
    .source_tree(tree)?
    .cargo_lock_sha256(lock_sha256)?
    .dataset_sha256(dataset_sha256)?
    .topology(Topology::new(3, 3, 6)?)
    .postgres(PostgresEnvironment::new(17, true, StandbyMode::Synchronous)?)
    .machine(MachineEnvironment::new(8, 32 * GIB, "nvme")?)
    .median_database_rtt(Duration::from_micros(1_500))?
    .build()?;

let run = QualificationRun::start(
    QualificationProfile::ReleaseCandidate,
    QualificationWindow::release_default(),
    environment,
)?;
run.begin_measurement()?;
run.recorder().record_latency(LatencySignal::AdmissionCommit, elapsed)?;
run.recorder().record_counter(QualificationCounter::Accepted, 1)?;
run.begin_drain()?;
let report = run.finish()?;
let envelope = report.to_integrity_envelope()?;
```

`CiReduced` permits short windows but always emits `release_qualified: false`.
`ReleaseCandidate` requires the documented reference environment and at least a
10 minute warm-up, 30 minute measurement and 5 minute drain. The API cannot
accept caller-supplied elapsed phase durations.

## Detailed semantics

Profiles, scenarios, signals, counters, objectives and fault cases are closed
versioned enums. Unknown serialized values fail closed. Version one supports the
host-control-plane scenario and four shared latency signals: durable admission
commit, runnable-to-claim, committed event-to-SSE delivery and SSE reconnect.
Supplemental operations-read, recovery and replacement observations may be
reported but cannot silently substitute for a shared objective.

`QualificationRun` owns one monotonic clock and moves exactly through Warmup,
Measurement, Drain and Finished. An invalid or repeated transition fails. Only
events accepted during Measurement contribute to evaluated distributions and
counters. Events racing a transition take the phase observed while holding the
same synchronization boundary; no sample is split between phases. Finishing
before every minimum duration or with in-flight operations fails.

Latency values are finite integer microseconds in 1 microsecond through 1 hour,
recorded in an HDR histogram with three significant digits. Every distribution
reports observed sample count, coordinated-omission-corrected sample count,
p50/p95/p99/max and non-empty recorded buckets. Callers recording fixed-rate
work must provide the expected interval; the recorder inserts corrected samples
and retains both counts. Overflow, out-of-range values and lock poisoning are
errors, never discarded samples.

Counters include offered, accepted, completed, rejected, harness-capacity
rejections, expected injected failures, unexpected failures, acknowledged
records lost, accepted stale writes, cross-tenant disclosures and duplicate
external effects. All additions are checked. Saturation and noisy-neighbour
fairness use integer basis points and microseconds, not floating-point equality.
The report separately names unavailable evidence rather than converting absence
to zero.

The shared objectives are evaluated against the documented scenario targets.
Any lost acknowledged record, accepted stale write, cross-tenant disclosure or
duplicate external effect is fatal in every profile. Missing required evidence,
an unexpected failure or an unexecuted required fault also fails the run. Latency,
error-rate, saturation and fairness thresholds are informational for `CiReduced`;
they become mandatory only for `ReleaseCandidate` with a successfully validated
reference environment. Release qualification also requires less than 70 percent
saturation, unexpected errors below 0.1 percent and all shared latency signals.

The fault matrix has stable identifiers and explicit planned, injected, recovered
and invariant-check counts. Version one covers database unavailability, Worker
loss, host rolling replacement, verifier unavailability and operations-policy
expiry. A case is complete only when injection happened, recovery was observed
and every declared invariant was checked. Extra free-form case names are refused.
Fault callbacks remain outside the crate so the harness never receives production
credentials or infrastructure authority.

All strings and collections have explicit byte/item ceilings and reject control
characters. Source commit/tree and SHA-256 fields require lowercase fixed-width
hex. Environment validation records CPU/memory/storage/kernel/container facts,
PostgreSQL major version, synchronous-commit/standby configuration, topology and
measured application-to-database RTT. Reference validation requires PostgreSQL
16 or 17, synchronous commit and standby, at least the documented node resources
and role counts, and at most 2 ms median database RTT.

Report serialization uses sorted maps and RFC 8785-style canonical JSON already
used in the workspace. The envelope contains the schema version, canonical report
bytes and their lowercase SHA-256. Verification recomputes the digest and parses
the strict report before returning it. Report IDs derive from canonical content,
not wall-clock randomness. Wall-clock timestamps are metadata only and never time
phase transitions.

## Persistence and migration

No database schema or durable business record changes. Reports are immutable
operator artifacts with schema version one. Readers reject unsupported versions,
duplicate fields, malformed bounds and digest mismatch. Retention, object-store
upload and attestations remain deployment policy. Removing the crate leaves all
runtime state unchanged.

## Security and privacy

The harness stores aggregate measurements and bounded environment labels only.
It refuses tokens, request bodies, tenant/Run IDs, endpoints, SQL, provider output
and arbitrary exception text. Test drivers translate failures into closed labels.
The crate performs no network, database, file, process or fault-injection I/O.

`CiReduced` and reference-environment validity are serialized facts and cannot be
overridden at report time. Integrity is not authenticity: release evidence must
be bound to reviewed source and execution provenance by an external trusted
system before a production decision. Reports are safe to inspect but may reveal
aggregate capacity and topology, so publication remains an operator decision.

## Observability and operations

Operators retain the canonical envelope plus source commit/tree, lockfile and
dataset digests. A release run uses deterministic providers, a controlled client,
the documented topology, 10/30/5 minute phases and the complete fault matrix.
Alerts and dashboards are out of band; the report is final evidence, not a live
health endpoint. Saturation must be measured at the shared bottleneck and its
definition recorded by the driver.

The reduced CI driver uses actual PostgreSQL and Agent host paths with much
smaller load and time bounds. It must verify non-vacuous admission/completion,
operations visibility, dependency failure/recovery and replacement invariants,
and upload its exact report. Its objective status is diagnostic and it cannot
advance production readiness.

## Compatibility

Rust 1.88 remains the MSRV. The new unpublished crate adds pinned
`hdrhistogram` 7.6 with default features disabled; it has no runtime dependency
edge. Existing public crates, wire protocols, database migrations and Cargo
features are unchanged. Report schema evolution requires an RFC and a new version;
version-one readers do not guess how to interpret later schemas.

## Alternatives considered

Shell scripts and benchmark logs are smaller but cannot enforce phase ownership,
bounds or deterministic evaluation. Prometheus/OpenTelemetry histograms are
valuable operational outputs but do not bind source/environment/fault evidence
and require a telemetry deployment. A custom percentile vector is simpler yet
scales with sample count and makes coordinated-omission correction easy to omit.
A full orchestrator would exceed StateKnot's authority boundary; callbacks and
drivers keep infrastructure control explicit.

## Validation and rollout

Require unit and property tests for phase races, bounds, overflow, percentile and
corrected counts, integer objective boundaries, environment validation, every
fault-matrix transition, deterministic bytes, round trips, unknown fields and
tampering. Fuzz-like adversarial JSON tests must prove bounded rejection.

Require a reduced real PostgreSQL 16/17 test using the concrete Agent host and
protected operations. It must admit work through HTTP, observe terminal state,
make the business host unavailable during dependency failure, recover without
lost or duplicate work, start a replacement host before draining the old host,
and produce a verified non-release envelope with non-zero evidence. Preserve all
existing identity, policy, HTTP, Worker, maintenance and host gates.

Pass locked workspace tests, strict Clippy/rustdoc, dependency/license audit,
bilingual website build and browser/accessibility tests. A later production
qualification must run the full reference profile, 24-hour soak, database and
object-store failover, restore and N-1/N-2 upgrade matrix; those remain separate
release evidence. Static documentation may be deployed after merge, but no Agent
runtime or production-readiness claim follows from this RFC.

## Unresolved questions

No unresolved design choice blocks the experimental implementation. Evidence
signing/provenance, orchestration, telemetry export and the full release driver
require later decisions. This RFC remains Draft and cannot establish a stable
public compatibility or production SLO commitment.
