<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Original absolute Graph Wait recovery

Experimental, unreleased schema-36 contract under [Draft RFC-0022](rfcs/0022-namespaced-graph-frames.md#recovering-original-absolute-wait-conditions). These APIs are absent from `0.1.0-alpha.1`. Complete CI, fault/compatibility and production acceptance remain open. [中文](elapsed-graph-waits.zh-CN.md)

## Commit the original condition

A successful Pending result can survive beyond its absolute Timer due instant. Recovery preserves the complete original result, `NodeWait`, ID, kind and due time. It does not execute that successful node again, change its due time, backdate a registration or synthesize a firing.

Classification uses the actual whole-barrier event's database `recorded_at`:

| Original condition | Committed evidence |
| --- | --- |
| Timer `due_at > recorded_at` | Existing future-only registration with the original specification |
| Timer `due_at <= recorded_at` | Immutable elapsed-condition evidence, with no Timer registration or firing |
| Valid Interrupt | Original authorized registration and resolver policy |
| Expired Interrupt | Explicit rejection; no automatic approval or partial commit |

Every condition binds its original semantic ordinal, complete Pending owner, position in that owner's Wait array, specification/registration-intent digests, source journal and actual successor/event. Large Interrupt payloads remain in their complete authenticated Pending owner. Compact inventory references do not replace that owner. The original 64-condition, scoped 4 MiB and whole-transcript limits remain unchanged.

## Handle Waiting and Ready

Whole Root/scoped receipts expose `wait_effect() -> Option<GraphWaitEffect>`. A non-Wait receipt returns `None`; an original Wait returns its authenticated `Waiting` or `Ready` effect.

With any remaining registered condition, the transaction enters Waiting using only those real registrations and releases the original lease. Partial resolution and true firing retain the existing authorization and final-condition wakeup contracts.

When all original Timers were already due, the transaction saves the original Wait successor, consumes each exact original result once and saves every elapsed proof, while keeping Active lifecycle, its original revision and complete live lease. Ready is a durable observation, not permission to launch.

The scoped Driver updates its exact durable observation and obtains a fresh claimed plan before the next independent durable Start, within the original shared quantum. Root lifecycle and `DurableAgentLoop` return `ReadyReleased { receipt, release }`: the complete original Root receipt and ordinary release outcome for its original fence. `release: None` means that fence was already superseded; the replacement lease is not released. Scheduler discovery and a fresh claim perform subsequent business work. Callers must handle this new non-exhaustive outcome rather than treating it as Waiting or terminal success.

Lost-ACK recovery first authenticates the complete original whole transaction and its inventory, then returns that original classification. Later time, resolution, a higher fence or a Return cannot reclassify it, consume results twice, reset lifecycle or authorize another launch. Fresh writes flush component guards and re-read complete actual rows before the final original lease, database clock, deadline, journal, head and accounting checks under the same lock.

## Migration and deployment

Migration 36 adds immutable `graph_wait_conditions`. Its physical `(tenant_id, wait_id)` key arbitrates across Runs and Timer/Interrupt kinds for both existing registrations and new original-condition evidence. `legacy_registration` backfills only actual old registration metadata/anchors; it invents no graph owner. `graph_registered` and `graph_elapsed` bind complete original graph facts. A conflict must match the winning physical owner; absence after conflict is failure.

Existing Agent Root V1, trusted Root V2 and scoped V1/V2 bytes and owning readers remain intact. New V3 Wait facts bind their complete original source, plan, inventory and effect. Missing V3 inventory is corruption; legal legacy history is not assigned fabricated inventory. Root source authentication still verifies the full Agent admission or original FIRST/Start/INITIAL history.

1. Drain old workers and close their pools; retain a verified backup and exact old binary.
2. For current experimental binaries, run normal SQLx migrations through 37 with the migration owner. Preserve original checksums and bytes. Nonempty source-reconstruction tests and actual published-version tests are distinct evidence.
3. Install the matching current trusted-role profile and verify exact schema37, all 54 business tables and ACLs. Migration37 changes a closure guard, retaining the schema36 table/ACL inventory; do not reuse an older version's profile.
4. Start the matching reader/writer. Catalog mismatch refuses operation. New V3 facts cannot be served by old V1/V2-only readers; this is not a mixed-version rolling upgrade or an automatic rollback.

The separate [schema37 Store Join owner-stop record](qualification/experimental-scoped-join-owner-stop-2026-10-18.md)
describes its cancellation-only closure-v2 increment. Migration36 remains the
original WaitV3 introduction; current deployment through37 does not transfer
its historical qualification to the later source.

A restored schema must pass the same complete catalog and owning readers. Static checks, a conditional resource test entry, source-reconstructed schema32 and a single true published alpha.1 upgrade each have limited scopes; none alone qualifies the complete compatibility window or R1. Qualification evidence must identify exact source, binary, resources, actual markers and all original failures.
