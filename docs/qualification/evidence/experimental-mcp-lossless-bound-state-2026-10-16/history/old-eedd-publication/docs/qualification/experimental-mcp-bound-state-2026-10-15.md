<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Experimental MCP bound state: component execution record

2026-10-15. The final fixed candidate passes six strict gates and the full14-test MCP component suite on PostgreSQL16.15/17.11 with the actual serial command. This qualifies the bounded opt-in component evidence below, not full CI, a production identity backend, stdio, Supported status or R1. [中文](experimental-mcp-bound-state-2026-10-15.zh-CN.md).

The [machine record](experimental-mcp-bound-state-2026-10-15.json), [artifact index](evidence/experimental-mcp-bound-state-2026-10-15/artifact-index.json) and [offline verifier](evidence/experimental-mcp-bound-state-2026-10-15/verify.py) bind source, actual terminal logs and limits.

## Exact component and host boundary

`McpServerHostBinding` carries trusted authenticated issuer/subject, local tenant, effective principal, nonzero mapping generation and deployment namespace. The optional `McpServerBoundStateProfile` uses one identity-and-scope fingerprint for private catalog cursors. The three Tool/Resource/Prompt MRTR consumers additionally use strict state envelopes and operation-specific associated data through the configured codec. Binding changes, generation/ABA or scope changes, malformed/old/tampered/expired incoming state refuse before business dispatch. No fallback from the strict profile to legacy state is inferred.

The test host checks its current mapping after decoded-policy queueing and in catalog admission. That current lookup, authentication provenance, generation persistence/non-reuse and deployment policy remain host responsibilities; typed values and a state fingerprint do not create authority. Authorization has a linearization point; already-authorized in-flight work or released bytes do not gain zero-window revocation.

The profile is explicit opt-in. The legacy `McpServerPrincipal::new(subject, scopes)` constructor, host-bound but unconfigured services, and old clones retained before configuration are not implicitly upgraded. Direct/shared/composite configuration order and clone preservation are tested. Private catalog metadata does not change independently explicit Public Resource contents. Missing incoming applicationState differs from explicit null. Outgoing aggregate-size refusal happens after the business callback; it does not prove business rollback or authorize retry. Existing TTL/size limits, effect handling and full Store authentication remain.

## Source, build and actual execution

Frozen928-file tree: `eeddc9f7e9c92023a0b0c1f9a7220cccc1b4c5d846ae4281d4a4aeff9b0f30f4`. Reconstruct it from the already public A2A923 archive plus the public single PITR helper to obtain3a84, then the [exact twelve MCP files](evidence/experimental-mcp-bound-state-2026-10-15/source/final-reconstruction.json). Eight production files and four facade test files change; five files are added. No new complete source archive, binary or cache is copied. The existing531 conservative supplied inputs become536, canonical digest `31b3be83c5a1d38e8204d6d2fd9caf7989206b93d401aeac4045149e7f298807`.

This is a crate/config/fixture input superset, not a compiler-read closure. The old frozen `.gitignore`, docs and workflow are not actual main85f or adoption candidates. Adopt only the twelve listed source files; do not copy the whole928 tree. Cargo dependencies/lock, Core/Store/Runtime production, migrations, SQL, original resources and default stack are unchanged.

| Execution | Actual result |
| --- | --- |
| Six strict gates, Rust1.88.0 and pinned nightly2026-10-07 | all exit0 |
| Workspace all targets/features, original opt0 | 99 suites;2095 passed;0 failed;6 original ignored |
| Independent doctests | 48 passed;0 failed |
| Fresh saved per-package default-feature Native program | opt0/debug0; assertions and overflow checks true; fresh:false |
| PostgreSQL16.15, full serial inventory | 14 passed;0 failed/ignored/filtered; marker exact1;35.27s |
| PostgreSQL17.11, full serial inventory | 14 passed;0 failed/ignored/filtered; marker exact1;35.15s |

The Native binary SHA256 is `3d3b32d05d1b442c2e72953e5bdee2f70c0ac3119724ef747b4d3d54810ba2f6`. Actual arguments are `--nocapture --test-threads=1`; full Cargo JSON, inventory, command receipts and both logs are retained. Workspace gates clear DB environment, so conditional returns there are not Native evidence. Native uses mandatory new task databases and contains13 non-DB cases (12 TCP cases and one construction/configuration case) plus one durable case, not14 separate DB scenarios.

Real public TCP paths cover the three consumers, private pages, rebindings/scopes/ABA, strict envelope presence/version/tamper/expiry and size boundaries, direct/shared/composite configuration, legacy behavior and queued current-policy checks. The durable function makes six genuine commits and refuses twelve rebinds. It compares the complete two fixture-owned `StoredRun` representations and every typed `JournalEvent`, traversed in pages of2; these fixture runs have no checkpoint or lease side ledger. It asserts the other tenant's full fixture ledger remains unchanged. This is not a snapshot of all database tables or all tenants, and journal payload dumps are not exported.

The48 Rustdoc tests do not qualify the separately authored Markdown guide example. Seven adoption documents have a separately frozen Standards0/Spec0 content review, independent of documentation authorship; it does not certify source/test implementation. This component does not inherit the old full Store/Runtime Native, A2A facts or opt1-union qualification onto536 inputs. No speedup, capacity or CPU-percentage conclusion follows.

## Preserved failures and review

The first strict Clippy run exits101 with three lint diagnostics; the next exits101 with17. Bounded wire parsing moves behind a private helper with its original complete validation phase retained. The next workspace run exposes a fixture HTTP400/SDK JSON-wrapper parsing error (8 pass/6 fail), reproduced as exact1 failure. The fixture fix preserves production checks and adds the actual HTTP400 assertion. The first PG16 Native run then has13 pass/1 fail because the fixture event kind contains an invalid dot; PG17 is not run. Replacing that single test literal with a legal hyphen produces be326, whose six gates and both default-parallel14 runs pass.

An actual old-binary **serial exact1** reproduction exits0, but its marker shares the libtest pretty line; the original anchored checker sees0 marker lines. This remains a checker/fixture red. The final change inserts only literal backslash+n before that existing marker. Other927 files, all eight production files, assertions and resources remain exact. Final eedd is then freshly built and its full serial14 and six gates actually pass. Raw failures, manifests, exact fixes and compact source reconstructions remain; no threshold, checker weakening, resource increase or production bypass is used.

Standards0 and Spec0 cover fixed be326, with the final marker-only increment separately reporting Standards0/Spec0 and closing the original CI marker P2 on both actual new serial logs. These static reviews do not themselves execute or qualify CI. The publication bundle and adoption-document reviews are separate scopes. A separate0-remaining execution-fact check is independent of the runner/collector; its reviewer authored the original tests, so it is not counted as independent source/test Standards or Spec.

## CI and resource limits

The prepared [CI candidate](evidence/experimental-mcp-bound-state-2026-10-15/CI-candidate/receipt.json.gz) has SHA `6568a90f4de078ef9f4bc7469e7891b72af7b00619301ee645dced64fea14fd9`. It keeps the original53 steps/31 run commands and adds two steps/one run:55/32, still30 minutes. Existing job env, images, all old checks and fixture resources remain. The new step requires the complete14 summary, exact marker and no skip, preserves Cargo failure and always uploads its evidence.

That CI job inherits the already adopted test opt1 environment; this local component was tested at opt0. Ubuntu, testopt1 and the entire unchanged-budget job remain pending actual CI execution. Matching feature defaults or a successful checker replay cannot transfer profile/platform/full-job qualification.

The final read-only resource observation covers the saved Native program directory and reused stable/nightly task caches: lsof is empty and cache locks are acquirable. Actual df and logical cache sizes are recorded separately; missing-tag caches outside this scope stay retained. Logs/programs are outside caches, and no cleanup or DB lifecycle action is performed. This is not an entire-machine process or all-database cleanup claim.

Run the offline verifier first. After adopting the twelve source files and reviewed workflow, `--workspace` also checks the complete actual536 supplied-input path/byte/hash set. It must reject before adoption or after later compiled-input changes:

```console
python3 docs/qualification/evidence/experimental-mcp-bound-state-2026-10-15/verify.py
python3 docs/qualification/evidence/experimental-mcp-bound-state-2026-10-15/verify.py --workspace
```

Original text is retained with deterministic gzip and exact raw hashes. The verifier executes no build, network or database command.
