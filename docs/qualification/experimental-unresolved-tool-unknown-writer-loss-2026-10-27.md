<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Unresolved Tool Unknown across writer process loss

Updated: 2026-10-27. This test-only profile extends experimental RFC-0022 evidence without changing production Rust, public APIs, SQL or support status.

The killed writer itself executes a real durable Tool attempt, persists Unknown/ReconcileFirst and the parent failure-close/child cancellation facts, then hands its actual receipt to the test controller. No reconciliation consent, child terminal usage or settlement exists at this cut. The controller kills and reaps that original writer. Retaining its receipt in the test controller does not prove an independently durable external provider.

A new cold process receives only the lookup key and invocation ID, with no provider or receipt. It waits for the original 30-second lease to expire naturally, claims a higher fence, and runs the real DurableGraphDriver. The observed handoff is CancellationRequested with zero application calls and zero durable events. It then explicitly releases the claim and is also killed and reaped. Claim, renewal and release are real permitted writes: only the exact child's lease/scheduling columns may change. Complete accounts, original Unknown history, acknowledgements and all other tenant facts remain protected. Owning reads and rejection checks each establish their no-write window after those permitted operations.

Only after the cold proof does the controller rebuild a test provider from the original receipt. A denied probe preserves state; explicit consent for the original invocation/schema/operation then permits reconciliation. Assertions separately observe one business send by the original writer and zero by the recovery provider, two probes and one denial. Original result/ACK replay is unchanged. Independent complete child settlement, delegated USD cost of 11 micro-units, parent/child accounts, terminal deltas and complete acknowledgements are checked before the final no-write rereads.

The measured local code candidate has 992 source files `f2089d5a48d9288cc1241b0674235d17d13a024751b41fb0e9f0249092c6d939` and 565 compiled inputs `7964eb40eee79683f35c5ec6cb9677bf0751f597e031065261ca92842617df30`. Six Rust gates pass. The original strict-Clippy similar_names failure and its six-identifier-only successor remain retained. A real default-feature Native build lists Store276/Runtime233; the exact controller passes on macOS aarch64 with PostgreSQL16/17, each 1 passed/232 filtered, in 34.29/33.98 seconds. Lease30, READY90 and the default stack remain unchanged.

The complete original evidence packets are `r1-unresolved-unknown-writer-loss-native-build-v1` (manifest `235f484f7a0bc4ac896fd1137aab4565a643a83be7bee2bf51259dcde4df44fa`), `r1-unresolved-unknown-writer-loss-pg16-v1` (`d69f28dbb5e4c8793aa4016a6a11266671e4b7b6956751a1f06e5c88c5175489`) and `r1-unresolved-unknown-writer-loss-pg17-v1` (`b6d8a0800d26812737332f50ae179a9b3e609f21776482b61c4af5fe11604df6`). Markers retain full account/ACK data and digest bindings; lease timing, SQL snapshots and probe counts are enforced by the executed assertions rather than separately archived raw snapshots.

This delivery synchronizes all current CI inventory/Return/Wait guards with the actual 233-case list and its explicit local provenance. Historical 229 and 231 fixtures remain. Runtime lanes derive 117/116 exact names; Store276, all step mappings, profiles, owner protocols, the 1,800-second pair and 30-minute job budget remain unchanged. The new documentation/CI composition needs its own validation and exact-head CI; the earlier code candidate does not grant that qualification automatically.

Unknown-transaction COMMIT ambiguity, independent provider durability, cross-OS kill qualification and the complete remaining fault matrix remain open. This does not close RFC-0022, R1, stdio or release qualification.

[简体中文](experimental-unresolved-tool-unknown-writer-loss-2026-10-27.zh-CN.md)
