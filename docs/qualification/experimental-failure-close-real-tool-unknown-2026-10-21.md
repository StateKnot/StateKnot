<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Preserve frozen DIRECT when a reconciled child introduces a currency

Generic failure close could reject a correct parent terminal total after a real Unknown Tool was reconciled and its child settled. If the verified failure-close DIRECT had no currency entries and the child spent USD11 micro-units, subtracting the child from the total retained an explicit USD0 entry. That representation did not equal the original empty DIRECT, so the finalizer returned IncompleteChildAccounting. The fix verifies the complete terminal total against original frozen DIRECT plus actual delegated usage and retains that exact DIRECT representation.

[中文](experimental-failure-close-real-tool-unknown-2026-10-21.zh-CN.md). Local directed RED/GREEN below is source-specific. The same GREEN source still has two test-style Clippy errors; the forthcoming style/metadata/docs combination needs its own complete gates. R1, RFC-0022 acceptance, stdio and release remain open.

## Production boundary

Only two private Store implementation paths change from the saved RED:

- [failure_closes.rs](../../crates/stateknot-store-postgres/src/failure_closes.rs) resolves terminal DIRECT from the existing verified RunFailureClose record. It requires full total equality with frozen DIRECT.checked_accumulate(actual delegated usage); an incorrect total still rejects. It returns the original full DIRECT rather than reconstructing its representation through subtraction.
- [child_runs.rs](../../crates/stateknot-store-postgres/src/child_runs.rs) uses that resolver before existing frame validation and account.observe_direct at the actual terminal ACK head. Child entries, settlements and delegated usage remain unchanged.

Core's deliberate explicit-USD0 subtraction semantics, public APIs, SQL, dependencies and wire schemas are unchanged. Without a verified failure-close record the prior subtraction remains. Ordinary frame/noFailure and Root-barrier instances of the same representation issue remain open; this generic RunFailureClose fix does not qualify them.

## Actual directed evidence

The identical complete Runtime test file (SHA256 `2730d90fccd8137f8583f57bac67f6cff3592cb6c9e1c23c0b2e178287d73374`) ran before and after the two production changes. The characterization was appended to the existing Runtime failure_close.rs in the 6ec baseline. No SQL manufactured result, terminal or settlement facts.

| Binding | Saved real RED | Fixed minimal GREEN |
| --- | --- | --- |
| Selected source | 972 / `6d9b7e6a37eb90181b2a69175b2123f9a86ff330a08049820d922ea4e17c7098` | 972 / `dcf0a54436606790093882744f3384fc78346ede72c4ab6d9eef318d0b745ed8` |
| Compiled inputs | 563 / `87fc37c226b7ecc00a0911448b9775c738b0eb65893dca816b44c50e9938531e` | 563 / `88e00f7550cbe843c4e57145fd909be2b95688f5df41a7c0247b4569b0a6becd` |
| Root index | `420a34aa59e6ee3ba6df853ccdeb7ccf88b0a72b178379cfd2134147e7927ce0` | `523ca62e38dee1e35714d66e0df6935692dc90712ec34d21bd90df69108f4a0f` |
| Each PostgreSQL16/17 exact | exit101;0passed/1failed/228filtered | exit0;1passed/0failed/228filtered |

GREEN preparation is `6dc564b312539e98714bd7bdf2d8f1343dd699e6736926274b00b6b115edad14`; build `86253ca349107c36d65ae2a5175123a8a03cec545f1640bd2080ffbaf7212abc`; Runtime binary `00caca25bb06cd770bcc29e26238d9883bbcf0328cbb0c95d5874b50233dbae5`. Actual full build inventories were Store275/Runtime229; these captures executed only the one exact Runtime case, not either complete suite.

| Local GREEN capture | Wall time | Capture manifest |
| --- | --- | --- |
| PostgreSQL16 | 3.765131s | `bebd985f53c309d4d9bdc7e2b3daf55f643994dd7bbd5c7bbe509cd28782e0d0` |
| PostgreSQL17 | 3.707860s | `999f6c95c1a0ee87e153598cb54210152a2ff22808a6ed70f1a32d0e44b6dfb5` |

Each95-indexed/96-physical capture contains one detailed marker,58true checks and bothA/B profiles: four profiles/116true checks total. Source/binary/primary bytes and mtimes, container identity and durable settings remained exact. The harness was local macOSaarch64 with PostgreSQL containers, original default features/stack, testopt1/debug0/assertion+overflow checks and original lease30/READY90/pair1800/Native30min limits. This is not Linux-host, performance/capacity or OS-kill evidence.

## Tool Unknown, consent and accounting

**A: parent Unknown.** A real admitted child first executes a known Tool and is publicly cancelled/settled. Only then does the parent Tool store its fixture-backend receipt and return real Unknown/ReconcileFirst. The real parent node failure is durable while the Run remains Active. Before each failure-close request, a fresh complete tenant/Tool/account/ACK snapshot is taken; exact CheckpointBlockedByToolInvocation leaves it unchanged. Its negative candidate usage is not called complete DIRECT.

**B: child Unknown.** The child Tool reaches real Unknown. The public parent Loop registers its original failure-close and the original reconciler saves child RequestCancellation before each baseline. UnsettledChildRuns rejects completion without changing the pending parent decision, Unknown record or unsettled child account. After explicit authorized reconciliation the public child Loop confirms cancellation and the reconciler settles the original child once. The RED's A controls completed; both B parents were rejected at finalization despite correct totals and settled children. Actual saved DB observations and the unchanged Core test established the currency-representation cause, not a setup or timeout inference.

Reconciliation first denies an unconsented probe without tenant writes. Current explicit consent binds original invocation/attempt, descriptor/input/schema, operation, tenant/run/thread and original Executing origin; the real provider probe then returns the original test-backend receipt. Each invocation has one business send across Store/Runtime reconstruction and reconciliation. A has two distinct invocations, each sent once. The backend is an in-process fixture; this proves the selected no-resend profile, not arbitrary external-network exactly-once or external-backend durability. Trusted commit_tool_reconciliation only replays the original full reconciliation ACK and is not itself a provider probe.

The complete reader uses actual admission input, canonical journal bytes, checkpoint, failed-node usage and reconciled Tool history/input/output/artifacts with an explicit known-price policy. Parent DIRECT excludes child subtrees. The full child terminal usage, including topology peaks, stays in its immutable settlement; delegated usage is cumulative_only. In both majors A retains DIRECT USD11 plus child USD11 for totalUSD22; B retains DIRECT costs[] plus child USD11 for totalUSD11 (all amounts are micro-units). Child topology peaks are not inherited. Terminal confirmation advances the account DIRECT head/digest to the original terminal ACK; full expected account is preaccount.observe_direct(actual ACK head, frozen DIRECT), with original child entries/settlements/delegated usage unchanged.

Original failure-close registration, child terminal, parent terminal and reconciliation ACKs are preserved in full. Explicit release, Store reopening and Runtime reconstruction precede normal claim; subsequent original ACK replay preserves every tenant fact and the complete post-terminal account without writes. This is same-process object/Store reconstruction, not process replacement or natural-expiry recovery. Fixture cleanup/pool+Store closure precede final verdict and marker.

## History and remaining gates

The initial characterization design predated the discovered production defect. Authoring-v2's two static accounting-oracle findings and formatting-only source remain retained and are not functional RED. The first build had nine API type errors and never executed the DB case. The later API-compatible source produced the actual paired RED above. Core's existing test intentionally preserves explicitUSD0; the fix does not normalize that global behavior.

Strict Clippy on the directed GREEN source actually exited101: only the new close_unknown_usage and close_unknown_finish exceeded100lines (101/105). Log SHA256: `5ebd35ca0f4ad6e1080b9599ee476e3559915b4ffc7d7612f9fa26122c9c4434`. A separately authored source-only helper extraction and six-file inventory overlay are planned; their combination with these five docs has no predicted source/compiled hash or passing result. It must run its own fmtcheck, strict Clippy, workspace/doc/nightly gates, complete PostgreSQL16/17 Store275/Runtime229 and new publication/CI. No full-source qualification transfers from minimal GREEN.

The detailed `STATEKNOT_FAILURE_CLOSE_REAL_TOOL_UNKNOWN_EVIDENCE=` marker was captured only by local exact --nocapture. Full Native retains default libtest capture and does not naturally collect a successful marker; actual full inventory, case exactly once and complete harness assertions are its execution proof. No new mandatory process step, partition, artifact, ignored case, fixed sleep or budget expansion is added. The published6ec baseline's 2,171workspace/48doc and1,006complete Native passes remain historical. Its CI39464102714 ended21success/1WindowsNoRunner-cancelled;56artifacts/3613indexed files were saved, but collection_complete=false,15components incomplete,117controls unexecuted and fullCI=false. Parent53 CI39444455774 and111offline controls likewise do not transfer.

Broader scoped Unknown/effect and fault matrices, ordinary frame/Root-barrier fallback, independent Driver interruption, R1/RFC-0022/R6/R7, MCP stdio, Supported and release qualification remain open.


## 2026-10-21 retained complete Native attempts

The later combined D3 source974 `6baea426d90df70425c5f2879c5e26d4f1c1ae3d2983c58d082b856cac44c23e` / compiled563 `872a34f78142caeec96fb2980e4a18f5a6a72a7f7dcf106f07e8506e9601b031` did not earn full Native qualification. Root record b13205d3… retains all four complete raw watchdog captures: PostgreSQL 16 Store/Runtime reported 12/5 FAILED prefixes; PostgreSQL 17 reported 3/6. Each binary exited -15 at the original 1800s limit; none produced a complete harness summary. These prefix counts are not complete failed-suite totals. Earlier directed GREEN remains source-specific and does not override those failures. No unique environmental or source cause is claimed.

The subsequent [D4 closed-frame record](experimental-closed-frame-direct-costs-2026-10-21.md) belongs to a different source 16863bd8… / compiled 48eccc16…. On that source the original D3 exact case passed in each major (1 passed / 228 filtered), four A/B profiles / 116 true checks; this is a regression result, not a rerun or acceptance of the four old full suites. D4's same-source strict Clippy still exited 101 for three Store test helpers. Final combined gates, full Native, publication/newCI, R1/R6/R7, stdio and release remain pending/open. Every earlier paragraph above is retained as its historical evidence cut.
