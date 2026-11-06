<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Experimental PostgreSQL test profile: bounded execution record

2026-10-15. Six strict gates and ten Native suite commands exited0 under a command-local test opt1 profile. **The runner exited1** at its final cache guard because both new cache roots lacked `CACHEDIR.TAG`. Its original failure is retained, no original success summary was created, and no tag fix, cleanup or retry occurred. This is a bounded product execution record with a separate successful read-only resource audit, not an overall green qualification. [中文](experimental-postgres-test-profile-2026-10-15.zh-CN.md).

The [machine record](experimental-postgres-test-profile-2026-10-15.json), [raw artifact index](evidence/experimental-postgres-test-profile-2026-10-15/artifact-index.json) and [offline verifier](evidence/experimental-postgres-test-profile-2026-10-15/verify.py) retain the exact boundaries.

## Source and build

The frozen923-file tree is `3a84c68eb9dd9df1804a7e24801516e11c9ee01ca0c6f243a4fdfaf76cbe124c`. Reconstruct it from the already published [A2A923 archive](evidence/experimental-a2a-request-facts-2026-10-15/source/source.tar.gz) plus the [single PITR test-file delta](evidence/experimental-postgres-test-profile-2026-10-15/source/reconstruction.json); this bundle adds no complete source archive or binary bytes. Production code, SQL and Cargo source profiles are unchanged by this experiment.

All531 conservative crate/config/fixture inputs match the composed main before and at final verification; the canonical input digest is `ddaf1aba9089af825b870d3c1466969d8967d59ee8805564b302b4c16eebc4b8`. This is a supplied-input superset, not a compiler-read closure or whole-main-Git-tree equality. The prior [opt0 record](experimental-composed-request-facts-pitr-2026-10-15.md) executed2182 actual main files. Its six-gate receipts bridge through531 exact inputs; it did not execute this whole923 tree or an exact composed full-Native opt0 baseline.

Command environment: job-local build incremental0 and dev/test debug0, **test opt-level1, debug-assertions=true and overflow-checks=true**, strict Rust/Rustdoc warnings, pinned Rust1.88.0 and nightly2026-10-07 check. Default stacks, leases, deadlines, quantum, fixture resources, SQL and complete authentication are unchanged. The raw six-gates summary's dev/release shorthand refers to unchanged source configuration, not an unchanged dev debug environment. Build scripts/proc macros retain original build overrides; not every artifact is opt1. Fresh compiler JSON, library profiles, five binary SHA identities and full inventories are retained outside caches.

This build used **workspace all-targets/all-features dependency union**. Each package's empty feature list does not make it the original CI per-package default dependency graph.

## Actual execution

Six strict gates exited0: workspace2073 passed/6 original ignored, independent doctests45 passed, fmt, strict full-workspace Clippy, Rustdoc and pinned nightly. Static conditional DB entries are not Native evidence.

| Complete Native suite | PG16.15 | PG17.11 |
| --- | --- | --- |
| Store264 | 446.94s | 454.46s |
| Runtime189 | 719.32s | 687.62s |
| provider2 | 1.22s | 1.19s |
| DeepSeek example1 | 1.50s | 1.51s |
| additional `mcp_durable` facts6 | 1.67s | 1.64s |

Every row has0 failed/ignored/filtered. Runtime189 includes original conditional worker/resource entry points and harness self-tests; its total is not189 independent failure profiles. Only actually enabled scenarios and their explicit markers grant their stated narrow coverage. The original four suites remain distinct from the additional facts suite. Both original default-stack seven-level, serial sibling and shared-quantum regressions pass. All15 original Runtime marker categories match their required counts; facts contain MCP2+A2A2 denial/lost-response markers with complete durable-start, original input, policy/send and ledger assertions.

These local serial durations confer no speedup percentage, capacity, CPU share or CI budget claim. The experiment did not enable its conditional logical-backup/PITR profiles. Five saved programs, all recorded PIDs and both caches are inactive; locks are acquirable. Both cache tags remain missing, so caches were retained without cleanup eligibility. The audit records actual df and logical sizes separately. The read-only audit made no DB/volume lifecycle changes; the two new task databases and prior volumes were retained. The runner's final guard failure remains a failure.

## Original CI and proposed next configuration

[CI38741332898](https://github.com/StateKnot/StateKnot/actions/runs/38741332898), head686c427, ended with11 successful jobs and two ordinary PostgreSQL jobs cancelled. Both raw annotations state the original30-minute limit was exceeded. Store passed264 on each major (1323.88/825.27s); Runtime entered189 but neither log has its terminal summary or an explicit FAILED line. Provider/example and later qualification steps were skipped. This is incomplete CI, not an assertion failure or complete pass.

The actual PR merge checkout was `b552ef62bcb9540fc4b0291b847665885201be5b`; its complete tree equals head686c427 at `222152eb603e960cadbb5001a80294b9de96bc84`. Separate original-profile Linux x86_64 PITR jobs pass exact1/188 filtered on PostgreSQL16.14/17.11, including cleanup markers. Their original two artifact zips and source binding are retained. They qualify that narrow profile only, not full Runtime or local opt1 PITR.

A nine-line job-env candidate (v2 changes only the overly narrow v1 comment) sets the same local test opt1/checks/debug/incremental values only for the PostgreSQL matrix job. Its original53 steps/31 run commands, default features, images, fixture resources and30-minute budget are unchanged; other jobs and Cargo dev/release/publishing configuration are unchanged. Its execution and v2 comment-only increment have a separate independent fact review with no serious or remaining findings; that review does not certify this evolving publication bundle. At this record's freeze it is a proposed configuration. **The original full CI job under its actual per-package default build remains pending** and must establish its own terminal result within the original budget. The local union result is not transferred. RFC-0022/R1, Supported status and production qualification remain open.

Run offline validation first. After applying the reviewed workflow and six candidate documents, `--workspace` also checks the complete actual531 path/byte/hash set and adopted docs/workflow; it deliberately rejects before adoption or after later compiled-input changes:

```console
python3 docs/qualification/evidence/experimental-postgres-test-profile-2026-10-15/verify.py
python3 docs/qualification/evidence/experimental-postgres-test-profile-2026-10-15/verify.py --workspace
```

The collector's first two verifier assertions about source/compiled manifest row ordering are preserved under tooling history. The correction changes evidence ordering only; no product execution or source was changed.

Raw text with original whitespace is stored with deterministic gzip; raw sizes/SHA remain verifiable. No build, network, DB or cleanup action is performed by the verifier.
