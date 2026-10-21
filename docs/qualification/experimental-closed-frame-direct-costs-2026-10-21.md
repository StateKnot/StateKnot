<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Preserve original closed-frame DIRECT when a child introduces a currency

[中文](experimental-closed-frame-direct-costs-2026-10-21.zh-CN.md). This record covers local directed evidence, not complete Native or CI qualification. R1, R6/R7, RFC-0022 acceptance, MCP stdio, Supported and release remain open.

## Production boundary

A real settled child carried USD 11 micro-units while the original verified cancellation closure had DIRECT costs[]. The correct parent total was rejected with IncompleteChildAccounting: subtracting child usage retained an explicit USD 0 entry, which differed from the original empty DIRECT. Both PostgreSQL majors reproduced this after successful setup; whole tenant state remained unchanged on rejection.

The fix changes three private production Store paths: child_runs.rs, failure_closes.rs and graph_frames.rs. It authenticates an existing failure-close or full owning frame-closure record, verifies the complete total equals original frozen DIRECT plus actual settled delegated usage, and retains that original DIRECT representation. Two available frozen sources must agree in full. When neither authenticated source exists, the prior subtraction remains; the original arithmetic-error priority, settled-account checks, SQL/lock order and historical ACK handling remain. Core's explicit-zero cost semantics are preserved. Public APIs, SQL, migrations and wire schemas do not change. This is a production fix, not merely a test change.

## Source-specific directed evidence

| Binding | Paired RED | Minimal GREEN |
| --- | --- | --- |
| Selected source, 974 files | `e3624bc4724f222adb897eb6a4b6799e2c0da9fbb9d6def51dc902df427dd2aa` | `16863bd82259968993a4686a810d7107dc06f2e4c328ab4fff50afd464da98b7` |
| Compiled inputs, 563 files | `1094d1d409fa0f7226813da9f88c993a6b8d71782910cf8a2e5cc19ea7dff57f` | `48eccc16d58f076e2395094010abb6a30ea1169c607c3b8426f08f47eb33e08e` |
| PostgreSQL 16.15 and 17.11, each exact | exit 101; 0 passed / 1 failed / 275 filtered | exit 0; 1 passed / 0 failed / 275 filtered |

The complete Store test file is identical across RED/GREEN (SHA256 `6becba10c74881a8a358f035d32c0599f3938e98e5fb2d75788668f498ca4276`). Paired RED Root record 83a8e2b5… and its independent evidence review 20bdff45… retain the original setup, failure and cleanup evidence. GREEN Root record c4713f09… binds preparation 936a4308… and build ab840711…; its actual inventory is Store 276 / Runtime 229. The owning closure, original owner, full child settlement, once-only account, terminal DIRECT head and original ACK/replay assertions ran before final cleanup/verdict. The two directed passes are not full 276/229 suite passes.

On the same minimal GREEN source/build, the D3 real-Tool-Unknown regression passed once per major (1 passed / 228 filtered), with four A/B profiles and 116 true checks (Root record b4cb757e…). Five additional existing guard cases passed in each major, ten exact runs in separate fresh databases (Root record bf13af38…). Together these are seven selected cases × two majors = 14 directed runs, not a complete fault matrix or full Native inventory.

The host was macOS aarch64 with PostgreSQL containers, default features/stack and Native test opt1/debug0/assertion+overflow checks. Original lease 30s, READY 90s, full-pair 1800s and Native-job 30min budgets are unchanged. This does not establish Linux-host, capacity or performance qualification.

## Retained failures and remaining gates

The older D3 combined source 6baea426… / compiled 872a34f7… has four saved full-suite failures (Root record b13205d3…). Each reached the original 1800s watchdog and exited -15, with complete raw capture but no complete harness summary. Observed FAILED prefixes were PG16 Store 12 / Runtime 5 and PG17 Store 3 / Runtime 6. These are retained failures of that older source, not results of the current 16863bd8… source; neither its earlier directed passes nor this D4 result erase them. No unique environmental or source cause is inferred. See the [D3 record](experimental-failure-close-real-tool-unknown-2026-10-21.md#2026-10-21-retained-complete-native-attempts).

Strict Clippy on minimal GREEN actually exited 101 in 35.227s: only three new test helpers exceeded 100 lines (settle_priced_child 106, terminal_and_replay 125, characterize 126); log cd81b678…. No production compilation diagnostic was reported by that invocation, but it is not a static pass. A separate wrong-total USD 12 guard draft is source-reviewed only and has not run or been adopted. Its old 69062-byte prefix label includes a separating LF; the original preserved file is 69061 bytes. The sealed draft and metadata discrepancy remain retained.

A successor combining that guard and coherent helper extraction needs its own source/compiled identities, formatting, complete six gates, dual-major full Native and new publication/CI evidence. Final combined source/static/Native/CI results and any Draft publication head are pending; none is predicted here. Ordinary Root-barrier cases and wider Unknown/effect/fault combinations remain separate open boundaries.
