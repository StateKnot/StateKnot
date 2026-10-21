<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental Driver acknowledged release qualification

DCO commit `ff081612853dcdb3094297542d3d0739e8383496`, tree
`0ee75edc263b54ffd7793f44248aa3b2d8c15271`, matches all 896 actual tested
source files. Their manifest SHA256 is
`4fa93cf9c8d7d8e7c3c910f82fb9a1110bff5f55a9c94d15eb63311c7662a1bb`.
This qualifies the stated component; RFC-0022 and R1 remain open.

[中文](experimental-driver-release-acks-2026-10-12.zh-CN.md) ·
[Machine-readable evidence](experimental-driver-release-acks-2026-10-12.json)

The original guardian drained an owning mutation but replaced its successful
released outcome with `NoActiveLease` or a pool error. A real scoped Join
registration lost-ACK regression exposed this. A separate public Driver
regression independently observed complete lease release and unchanged
lifecycle, journal and checkpoint, held the actual COMMIT response for 31
seconds, then reproduced the incorrect pool error. Both failures are preserved.

The Driver now retains a completed `ChildJoin`, `FrameWaiting`, released
`Yielded`, or scheduled `Deferred` result after its full Store ACK. `Cancelled`
retains a completed release only if external shutdown was already requested
before guardian cleanup. Internal guardian cancellation cannot hide a guardian
failure. Live-authority handoffs and `Deferred::Due` retain the original failure
behavior. Complete owning readers, original fences, clocks and writes remain
unchanged; the change adds no execution authority.

Seven real guardian regressions pass on each of PostgreSQL 16.15 and 17.11:
quarantine ACK, Start ACK with higher-fence takeover, the live-fence negative
case, renewal past the original execution deadline, completion, external
shutdown, and acknowledged ownership release. The same actual binary was used
for both versions. Original two-second execution leases, normal thirty-second
leases/pool timeouts, thirty-one-second ACK cuts and default stack remain.
Renewal cuts now require an observed actual executor call. Start and quarantine
prepare connections before beginning their measured two-second lease;
quarantine uses a real public higher-fence takeover after normal preparation.
The genuine pre-cut preparation failure stays archived.

The exact source also passes formatting, strict full-workspace/all-targets/
all-features Clippy, 98 workspace suites / 2,047 tests with six existing ignored
tests, 45 separate doctests, strict Rustdoc and the fixed
`nightly-2026-10-07` full-workspace check. Database-unconfigured workspace
entries are not Native evidence. Two independent reviewers checked Standards
and Spec against the fixed patch and the subsequent fixture-only changes,
finding no actionable issue. Full source, binary, original failures, logs,
reviews and receipts are retained outside build caches.

Thirteen ordinary Root Runtime regressions on this exact source pass eleven
and fail two on **each** database version. Both original five-second parallel
Driver cases time out after the three branch completions; they remain open.
The earlier schema34 ACK source's full Runtime suite separately reports
152 passed / 23 failed on PostgreSQL 16 and aborts with a default-stack overflow
during the higher-fence nested case on PostgreSQL 17. These failures are neither
erased nor attributed to the new source. The original six guardian passes
remain scoped to their earlier exact source.

Final schema35/schema36/scoped Child Join composition, complete four Native
suites, default-stack deep/serial/AgentLoop recovery, history cost, Linux,
process/COMMIT faults, released N−1/N−2 compatibility and R6/R7 production
qualification still require their own evidence. This report accepts no RFC and
makes no production release or Supported claim.
