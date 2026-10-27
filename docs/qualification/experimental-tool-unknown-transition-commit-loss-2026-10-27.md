<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Tool Unknown transition across real COMMIT loss

Updated: 2026-10-27. Experimental test evidence; production Rust, public APIs, SQL, wire formats and support status are unchanged.

The writer uses the public start/dispatch/terminal-commit path and makes one real fixture Tool call. Its actual returned error and receipt reach the controller. Only terminal persistence uses a separate, one-session PostgreSQL fault proxy. A one-shot controller phase gate holds the semantic Tool UPDATE until complete baseline reads finish. There is no ordinal COMMIT selection.

BeforeCommit withholds the COMMIT request: rollback and writer kill/reap retain the complete original Executing revision1. CommitResponse observes server COMMIT CommandComplete plus idle ReadyForQuery while withholding both acknowledgements: independent owning reads retain Unknown revision2, the exact original provider error/predecessor and complete journal/revision facts. The committed delta permits only the exact invocation/run columns and one event/revision; all accounts, ownership and other tenant tables remain exact.

For each cut, a new cold process receives only the ownership key and invocation ID. It naturally waits the original 30-second lease, claims a higher fence, loads original Prepared revision0 and the original Attempt from the Store, and uses an empty provider registry. The original attempt returns Recovered without dispatch; a different attempt is rejected. Executing is rejected by the Unknown reconciliation constructor. Exact claim/release columns are checked separately from complete owning-read/refusal no-write windows; stale release cannot remove the replacement fence. Both original writer and cold observer are actually killed/reaped.

Only the committed-Unknown branch then rebuilds the fixture provider from the controller-retained receipt. A denied probe is unchanged; current explicit consent permits reconciliation to Committed revision3 and exact original ACK replay. Recovery business sends stay0, with two probes and one denial. The Before branch remains unresolved Executing. No private terminal handoff is reconstructed and no automatic authorized disposition is invented. Both children remain Active/unsettled; this profile does not claim child terminal/failure-close completion, a scoped-leaf Tool path, or independently durable external provider receipts.

The tested candidate has 996 selected source files `ccf157dc180a1519c490d1bddc061ef00fdbb372f72655a44168536bc948f4b5` and 566 compiled inputs `a2b025e70cfc20bd674beafdd783dcaf3ade250facaa8adc2d9b4d7b9a63b59a`. Six local Rust gates pass under the original approved execution channel. The first two function-length failures, the sandbox loopback denial and post-build disk guard failure remain retained. A reused-cache/default-feature opt1 Native build actually lists Store276/Runtime235; it is not a cold build. PostgreSQL16/17 each pass the exact controller, 1 passed/234 filtered, on macOS aarch64. Lease30, READY90 and the default stack remain unchanged. Marker values enforce executed assertions; there is no separately captured OS/SQL trace.

Evidence packets are `r1-tool-unknown-transition-commit-native-build-v2` (manifest `ccbf7f631022710f2de2cb502dcd3139b468b1df7b7e77f2537773e3ceb8e72d`), `r1-tool-unknown-transition-commit-pg16-v1` (`c5617bb761fe619158bc59c792cc4a13a5bc4e5eb98f9017c81391bc50b3f994`) and `r1-tool-unknown-transition-commit-pg17-v1` (`034f0665f23c23c7c6589c683b9961ee00de7ab752b2804db1f256fa975ae685`). The bilingual notes/235-case CI composition need their own validation and exact-tree CI. The prior `0d0d7ab` passed all22 jobs in [run40493667861](https://github.com/StateKnot/StateKnot/actions/runs/40493667861); its 276/233 archives preserve 1,018 case results and belong to that earlier tree.

A separate one-line CI leader correction groups stderr redirection before opening transient /proc stat files; read success/failure and all owner cleanup requirements remain unchanged. Four causal probes, the existing 60+12 controls and both Bash syntax checks pass for that isolated change. Current CI keeps all jobs, profiles, step mappings, original lease/readiness/pair/job budgets and historical inventories; new Runtime lanes derive118/117 exact names.

Remaining scope includes authorized cold Executing disposition, scoped uncertain-effect combinations, the full remaining fault matrix, protocol identity/deployment, owned bounded stdio, external consumers and R1/R6/R7/RFC/release acceptance.

[简体中文](experimental-tool-unknown-transition-commit-loss-2026-10-27.zh-CN.md)
