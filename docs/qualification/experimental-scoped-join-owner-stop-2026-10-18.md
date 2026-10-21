<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental registered scoped Join owner-stop qualification

Evidence updated 2026-10-19. [中文](experimental-scoped-join-owner-stop-2026-10-18.zh-CN.md) · [Machine record](experimental-scoped-join-owner-stop-2026-10-18.json) · [Portable evidence and verification](evidence/experimental-scoped-join-owner-stop-2026-10-18/README.md).

The selected experimental public Store contract under [Draft RFC-0022](../rfcs/0022-namespaced-graph-frames.md#registered-join-handoff-during-whole-frame-cancellation) is qualified for the exact candidate below. Its complete [CI run 39185847363](https://github.com/StateKnot/StateKnot/actions/runs/39185847363) finished with all 21 jobs successful. Strict offline analysis returned actual exit 0, with `full_CI_success_verified=true`, `full_CI_qualification=true` and no analysis errors. Driver integration, Root ConfirmCancellation, R1, R6, RFC acceptance and Supported status remain open.

The runtime qualification binds immutable commit `bbd9dcf833a3446b822e8e1398c53d19cf3b9a72`, tree `126d95b5a80c703b781e270a157c5b81a4a3ad39`, parent `e372184fb1c80ffb9903ac0d9feb8380db51253e`. That measured commit contains the historical pending-V2 version of these documents. This evidence/documentation update is carried separately; its later Git commit must not be represented as the measured CI head. The exact measured commit was adopted locally with all 38 staged payloads verified, a clean index/worktree and DCO preserved. That adoption changed neither remote PR nor main; the PR remains Draft.

## Selected contract

After the unique latest ordinary owner in the actual current leaf registers an exact unpublished Join, its lease is released while its original attempt remains Executing. Cancellation can close the frame stack only after every child in the complete Root-bound lifetime account is terminal and settled. The coordinator supplies complete priced Run-wide DIRECT at the saved observation; structural and account DIRECT observations remain lower bounds. Delegated usage is added once. The proof grants no ordinary completion/result, Join publication or consumption, lease, launch grant or further worker authority.

The owning transaction authenticates the full Root/frame lineage, original attempt/history/fence, request/registration and complete ownership set. It locks settled terminal children in canonical account order, rechecks their owning records and selected Join, and retains deferred guards plus post-guard owning verification. Fresh quarantine, another unfinished ordinary owner, a superseded owner, unfinished work, unresolved Tool/Model effects, unpriced usage and unsettled children remain refusals. Sealed failure close keeps its strict checks. An already committed original decision is fully authenticated before fresh candidate fields; later operator quarantine does not invalidate that ACK.

## Source, tree and binary binding

| Input | Exact value |
| --- | --- |
| Composed-V3 pre-CI selected source | 936 files; `21c32c1446377a9691d63d835bb732b8d8cc6259e236c9cd0575dc13369f7a44` |
| Canonical compiled inputs | 544 files; `30d6c10fd88118297131a28c0e3b78420f51dec17206a718948a348c7424de81` |
| Cargo.lock SHA-256 | `fc36c3966dc2abf990fae6a01b633459d95fedae0fd7f77c429b5f04447b2e94` |
| CI Linux Store binary / inventory | `ed81c46b5b9a96b10a2aa4809e7ecca299ca85d9ad43d25e132c89a7a9c287a0` / 267 |
| CI Linux Runtime binary / inventory | `3575b2e82d7a2319bcaed052e72665ac644b980432bed95c8ca55c3f9d265a1c` / 194 |
| Current strict bundle manifest | `60fab0ff4c2cfee052da0353c0757e49acc780ca7b6d42c9ff0f6fa95c0dddf6` |

The 936-file manifest is a selected source domain, rather than the complete repository. Five CI files are bound separately: the workflow overlay and four new helpers produce 940 selected byte bindings. The official complete tree has 4,088 blobs / 4,604 entries. All 38 publication payloads are checked against their immutable prepared bytes with Git blob SHA-1 and SHA-256; every other final blob must match the official parent in mode, type, SHA-1 and size. Content SHA-256 is claimed only for bytes actually read in the selected source, CI overlay and payload domains.

Native CI uses Rust 1.88.0 on Linux x86_64, default features, test opt-level 1, enabled debug assertions and overflow checks, no debug symbols and the default thread stack. Each major has the original two actual live owners, Store and Runtime, with real execution overlap and both identities fully retired. All 544 compiled inputs, raw test inventories, build/binary hashes, profile, database durability settings and cleanup records were verified. The separate local macOS binaries retain their own hashes and outcomes.

## Actual execution results and preserved failures

| Source / execution | PostgreSQL 16 | PostgreSQL 17 |
| --- | --- | --- |
| V2 selected Store settlement | exit 0; 1 passed / 266 filtered | exit 0; 1 passed / 266 filtered |
| V2 public Store COMMIT-loss / cold process | test exit 0; 1 passed / 193 filtered | test exit 0; 1 passed / 193 filtered |
| V2 complete Store | exit 0; 267 passed | exit 0; 267 passed |
| V2 complete Runtime | exit 101; 193 passed / 1 failed | exit 101; 193 passed / 1 failed |
| V3 local complete Runtime | exit 101; 183 passed / 11 failed | exit 101; 182 passed / 12 failed |
| V3 CI complete Store | exit 0; 267 passed; 505.98s | exit 0; 267 passed; 620.48s |
| V3 CI complete Runtime | exit 0; 194 passed; 803.05s | exit 0; 194 passed; 969.80s |

All four CI full Native suites have zero failed, ignored or filtered tests. V2 Runtime failed the role test's stale expected schema 36 against actual schema 37. V3 changes only that test literal to 37, with a separate source/compiled-input delta review. Its local full Runtime runs still failed as shown; their raw failed-test names, stderr, summaries and receipts are retained. Local power logs contain 30 sleep events and one clamshell event in the bounded execution interval. This is correlation only, with no controlled awake rerun proving causation. The Linux CI successes are separate executions and do not rewrite those failures.

The selected COMMIT-loss case exercises public Store APIs, without a Driver. A proxy withholds the actual backend COMMIT response; an independent reader proves the committed v2 closure before the live writer is SIGKILLed and reaped. A separate cold process reads the same closure and recovers Existing from a new event ID, stale revision and zero candidate usage without changing tenant facts. The original owner remains Executing and Join unconsumed. Both independent validators passed, requiring exactly 22 typed marker fields and real signal 9. PG16's original wrapper exit 1 remains: its observer incorrectly required contiguous test/`ok` text around a valid nocapture marker, while the actual test exited 0. The original false observations and Root-transcribed outer-result provenance are preserved.

All six V3 local quality gates also exited 0: fmt, strict Clippy, workspace tests, doctests, warning-denying Rustdoc and the pinned nightly check. Workspace totals are 100 harness summaries, 2,128 passed and six existing ignored cases; doctests passed 48. These all-feature opt-level 0 runs have no mandatory PostgreSQL environment. V3 local existing-consumed-Join logical backup and physical PITR each passed on both majors, four actual exit-0 executions bound to the V3 source and local Runtime binary.

## Complete CI evidence

The strict analysis verifies every job and step state, all original 60 PostgreSQL source steps on each major, all 54 artifact ZIP digests and member inventories, 1,525 indexed raw files and 99 successful official GET receipts. The workflow has four qualification shards per major plus both mandatory aggregates and the other 11 jobs. No cancelled, skipped required, failed or missing component can qualify.

Both majors also passed the two real cut markers for frame Return and scoped Wait (`commit_not_forwarded`, `commit_response_withheld`), each with inventory 194 and exact 1 passed / 193 filtered. Each major has all 48 workflow marker contracts and 24 raw harness logs verified, including schema-37 real LOGIN/ACL, logical restore, named physical PITR with resource cleanup, the mandatory 15-test MCP bound-state execution, original protocol/example executions and real pinned TLS identity/rotation/revocation/host markers. Reduced-host output still declares `release_qualified=false`.

The first strict analysis actually exited 1 because its generic observer rejected legitimate ignored child-process fixtures; its complete bundle, raw inputs and failed receipt remain. V5 accepts only three exact source-bound child entrypoints with their original counts, successful parent tests and matching markers; all other ignored/failed/empty harnesses remain refused. Root prepared a new bundle and collected fresh complete official evidence before the actual exit-0 analysis. Product code, original logs and test outcomes were not rewritten.

## Migration, retained history and open work

Migration 37 replaces the complete closure guard, adds no table/backfill/ACL privilege and preserves the 54-table role inventory. The v2 envelope and intent add one required three-field `stopped_join`; v1 bytes, domains and getter remain unchanged. Register both exact offline schemas when consuming both histories:

| Getter | Version | JCS document digest |
| --- | --- | --- |
| `graph_frame_closure_event_schema()` | 1.0.0 | `sha256:453045e205133eb10d3cd58f0242ee2ebbd26dd78051b0888cd6a95f8a53e520` |
| `graph_frame_join_stop_event_schema()` | 2.0.0 | `sha256:aec9b96de74a4185f288e1425a59a7fb8d8eb4363378bbd5522b2500a261ea5f` |

Drain old workers, retain a verified pre-upgrade backup, run the normal checksum-pinned SQLx migration through 37, apply/audit the matching role profile and exact catalog, then start matching binaries. Raw function replacement or ledger edits are not deployment. The test revert fixture refuses retained v2 and supplies no production downgrade path. Existing-consumed-Join restore evidence does not establish owner-stop-v2-specific restore or published-version compatibility.

Earlier public-consumer schema/cold-read and retained source-v1 schema36→37 migration cases remain separate: product tree `bf329a3da4eacc9ccd9c34e681a1d53c8c9eab662e2c0a4b1831aac8f8266509`, consumer tree `5d77326d5cce4988506b7acb549cedb37eba5be62c14b857502736eeffaa806d`, binary `7c174940e83c9435e66ed7675b5faba4af3c8302c79732a648553b609350b30b`; all four exact cases passed. These source fixtures do not establish published N−1/N−2 or rolling-upgrade qualification.

The archive preserves all 806 original assembled files, the baseline PostgreSQL test-101/no-write evidence, manifest/Docker preflight failures, ACK/composed Clippy failures, loopback-permission workspace failure and all later Native failures. Historical CI runs [39113329120](https://github.com/StateKnot/StateKnot/actions/runs/39113329120) and [39120579635](https://github.com/StateKnot/StateKnot/actions/runs/39120579635) each remain 11 success / 2 cancelled under the original 30-minute limits, with their complete raw logs/ZIPs and independent reviews. Their Native inventories 264/193 and two/three-owner layouts do not transfer qualification to this candidate. Static reviews remain separate from execution evidence.

The first Store profile excludes published-but-unconsumed Join, spawn-before-registration and a later resumed physical owner. Broader negatives, deferred-guard/fault matrices, Root terminal confirmation, all three Driver guard removals, published-version/restore windows, required PR checks, capacity, independent security and deployment/release gates need their own evidence. Later ignored F2 slices, including inventories 197/198, remain independent future work and confer no qualification on this commit. The PR remains Draft; Driver, Root, R1 and R6 remain open.
