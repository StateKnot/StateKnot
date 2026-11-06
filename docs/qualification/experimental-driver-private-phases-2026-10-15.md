<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental Driver complete private phases

[中文](experimental-driver-private-phases-2026-10-15.zh-CN.md) ·
[Machine-readable evidence](experimental-driver-private-phases-2026-10-15.json)

The original two-level higher-fence recovery aborts on the default thread stack
on both PostgreSQL 16.15 and 17.11. Moving the complete Root readers alone still
aborts. The actual crash and binary prologues identify additive stack pressure
through the Driver, complete DIRECT observation and complete Store history.

The Driver now places its complete startup, iteration, barrier and node-batch
phases behind private heap boundaries. Complete owning recovery, scope, Root
checkpoint, active frame, plans, outcomes and DIRECT context stay on the heap.
The Root reader similarly retains complete admissions and checkpoints. Original
validation, SQL and lock order, cancellation priority, deadline, event quantum,
retry limits, guardian acknowledgements and release behavior remain intact.
There is no metadata-only authentication or persistent proof cache. The original
scoped ChildJoin refusals and complete Root Continue producer remain.

The final tested input contains 902 files, manifest SHA256
`2629e47bb772ae27ff506707cdc9fbacd7afcd0a0f016b2e63a1f7ee303e1d2b`.
Its Runtime Native binary SHA256 is
`e254cd98ae9bac08a82d13980d6155a642866d9c616bd4bc9e0f1a6a051796d1`.
The six adopted Rust files match that frozen input. The report, changelog and
milestone updates were added afterwards and receive no runtime qualification.

On this exact binary, the original two-level higher-fence recovery and full SQL
role-profile regression each pass on both database versions with the default
stack and original resource limits. The original seven-level test fails with
`LeaseExpired` on both: 1,115.07 seconds on PostgreSQL 16 and 1,042.00 seconds on
PostgreSQL 17. Its original final assertions are not reached. These concurrent
runs also overlapped compilation; their elapsed times are observations, not
capacity measurements. Seven-level recovery is not qualified.

Formatting, strict full-workspace/all-targets/all-features Clippy, 98 workspace
suites / 2,047 tests with six existing ignored tests, 45 separate doctests,
strict Rustdoc and the fixed `nightly-2026-10-07` workspace check pass. Workspace
entries without database configuration do not constitute Native evidence. Two
independent review axes reported zero actionable Standards and Spec findings
for the layout changes; token comparisons preserve complete phase bodies and
the Root SQL/literal order. Static review cannot establish runtime correctness.

An initial Root-only build reused the baseline artifact across source
directories. Its identical binary was detected and the result rejected. A
subsequent build in an empty target reproduces the Root-only stack abort. The
final binary was rebuilt after explicit cleanup of the task's workspace
artifacts; logs, source snapshots, original failures, crash reports and receipts
are retained outside build caches under `.local-verification/2026-10-15/`.

The next measured change targets repeated complete authentication during one
owning read. All physical evidence and fresh post-write validation remain
required. Full Native suites, schema36/scoped ChildJoin composition, Linux,
deep/serial/AgentLoop recovery, fault and compatibility matrices, capacity and
release gates remain open. RFC-0022 and R1 remain open; PR #161 remains Draft.
This increment grants no production release or Supported status.
