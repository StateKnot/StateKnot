<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental Driver guardian acknowledgement evidence

This component qualifies commit `32c130195399a10d6a9f9b0ee81323ea1ea75df4`,
tree `2c9128e37c19e24889b3e3ad9bccce34d9dd8922`: 884 complete source files,
manifest `127373c9cb1838429d0fd9c4bb93710d0bfc669d87514e8adf283b3af16a744b`.
Every committed blob equals the actual compiled and tested input. The later
ordinary Root/schema-35, elapsed-wait/schema-36 and scoped Child Join
compositions require separate qualification. R1 and RFC-0022 remain open.

[简体中文](experimental-driver-guardian-acks-2026-10-12.zh-CN.md) ·
[Machine-readable evidence](experimental-driver-guardian-acks-2026-10-12.json)

## Original failures and resulting behavior

The outer Driver guardian originally dropped owning recovery when its lease
observation failed. An actual PostgreSQL 16 quarantine COMMIT, with its ACK held
for 31 seconds, returned pool acquisition failure before the original quarantine
ACK. Cooperative stop now retains the full original drive and drains started
maintenance before returning. Complete owning readers and final live-fence
checks retain all dispatch authority.

The inner execution guardian originally dropped a committed renewal ACK at its
original two-second monotonic deadline. The actual failure and exact source,
binary and logs are preserved. Application tasks still stop at the original
deadline; the Driver then reads the complete historical ACK before returning
the original `LeaseExpired`. The late ACK grants no new launch authority.

The node-completion and external-shutdown regressions separately restore the
original execution methods. Both genuinely fail: two frontend messages reach
the same PostgreSQL session while the renewal ACK is held. The fixed methods
retain the connection and send zero subsequent messages until ACK release.
Independent database reads also verify the committed lease and unchanged
journal/checkpoint before release. Protocol contents and credentials are never
logged. The first green attempt's setup-time lease failure remains separate;
the two-second claim was moved after connection/catalog preparation, keeping
the original tested deadline and assertions.

## Actual checks

| Boundary | PostgreSQL 16.15 | PostgreSQL 17.11 |
| --- | --- | --- |
| Quarantine ACK after outer guardian failure | Pass | Pass |
| Start ACK with higher-fence takeover, zero dispatch | Pass | Pass |
| Start ACK with the original fence still live, cooperative stop | Pass | Pass |
| Renewal ACK after the original execution deadline | Pass | Pass |
| Renewal ACK before a completed node result can commit | Pass | Pass |
| Renewal ACK before external shutdown can release ownership | Pass | Pass |

Both six-case matrices use the same actual compiled binary and complete source.
The first compilation is recorded, rather than inferred from a reused cache.
Default thread stack, the original two-second test lease, the default
thirty-second lease/pool timeout and thirty-one-second outer cuts remain intact.
The exact source also passes workspace/all-targets/all-features strict Clippy,
98 workspace suites / 2,036 tests (6 existing ignored), 45 separate doctests,
strict Rustdoc, formatting and the fixed `nightly-2026-10-07` workspace check.
Workspace tests without the database environment do not qualify native cases.

One independent reviewer examined both Standards and Spec for the unchanged
production guardian methods. The later test-only protocol witness is not
presented as receiving that earlier review. Full source, binaries, diagnosis,
logs and receipts are preserved outside the build cache in
`/private/tmp/stateknot-driver-complete-ack-qualification-v1`.

The complete 175-entry Runtime native suite is running separately against this
preserved binary; this report does not claim its result. The final four-suite
native composition, deep/multiple-namespace Child Join and AgentLoop stack,
history-read cost, Linux, complete process/COMMIT fault matrix, released N−1/N−2
upgrades and production R6/R7 gates remain required. No lease/resource limit,
metadata authentication shortcut, RFC acceptance or supported-release claim is
introduced by this component.

A subsequent scoped registration COMMIT-loss matrix exposes a separate result
priority gap: complete original registration recovery finishes, but guardian
`NoActiveLease` replaces its acknowledged `ChildJoin` outcome. That exact-source
failure remains open and recorded in the JSON. The full Runtime run also exposes
a test-phase gap: counting global renewals can cut preparation before the actual
node call. The next test-only gate observes a real executor call while keeping
the original two-second duration. These results do not invalidate the six
actual selected passes or qualify the later repair.
