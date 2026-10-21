<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Published Join cancellation: public Loop COMMIT loss

This record separates the published baseline's complete CI from a new test-only candidate's local directed evidence. [中文](experimental-published-join-public-loop-commit-loss-2026-10-21.zh-CN.md) · [Machine record](experimental-published-join-public-loop-commit-loss-2026-10-21.json). R1, RFC-0022 acceptance and release qualification remain open.

## Published baseline

Head `53b0d83e226954c67004de5ff28dccd6208101e4`, tree `8d8793dee9cea47daf12cf875cab6f51d1c6074c`, has a verified complete [CI run 39444455774](https://github.com/StateKnot/StateKnot/actions/runs/39444455774), attempt 1. The actual checkout was `312e54f136b0ec625f25ede1eb4672c435a59546`.

| Binding | Observed result |
| --- | --- |
| Selected source / compiled inputs | 969 / 563; source `fbc2dc9c…`, compiled `3c6dd428…` |
| CI | 22/22 successful jobs, 20 checkout logs, 56 artifacts, 120 required step states |
| Strict analysis | 15 components and 111 negative controls passed; no pending positive targets |
| Complete Native suites | PostgreSQL 16 and 17: Store275 + Runtime227 each, 1,004 passed in total |

These results belong to that published source. The new public Loop case below was absent from its Runtime227 inventory. Old failed/intermediate evidence and its original limits remain unchanged.

## Local directed candidate

The isolated `published-join-v3-public-loop-minimal-source-v2` has 969 selected files and 563 compiled inputs:

- Source: `1424fc42fd41f3bdb9242b6b057572e4d1de9600904119b6a2c091c89cd8f41f`.
- Compiled inputs: `54c23f837b63b150aae70fa306cd1c2d6860b4c05ebc9e96b55af7d7eb0276d5`.

Only `scoped_child_join_fresh_red.rs` and `scoped_child_join_parent_terminal.rs` differ from the published baseline. Production runtime, public APIs and SQL are unchanged. Actual build inventories are Store275 and Runtime228; neither complete suite was executed by these directed captures. The build reused its cache and makes no cold-build claim.

| Local execution | Result | Wall time |
| --- | --- | --- |
| PostgreSQL 16.15 | 1 passed, 0 failed/ignored, 227 filtered; two COMMIT cuts | 82.832 s |
| PostgreSQL 17.11 | 1 passed, 0 failed/ignored, 227 filtered; two COMMIT cuts | 82.479 s |

The harness ran on macOS aarch64 with PostgreSQL containers. Native used default features and thread stack, test opt1, debug information 0, debug assertions and overflow checks enabled. The original 30-second lease, 90-second process readiness, 1,800-second suite watchdog and 30-minute Native CI job limit were preserved. A separate opt0 workspace/all-targets/all-features strict Clippy preflight exited 0; it does not complete the combined six gates. This is not Linux-host or performance/capacity qualification.

## Verified boundary

The writer enters through `DurableAgentLoop::run`. Its Driver returns the cancellation handoff before the Loop lifecycle begins the v3 closure transaction. Thus `public_loop_lifecycle_mutation_interrupted=true` and `public_driver_handoff_observed=true`; `standalone_driver_mutation_interrupted=false`.

Each PostgreSQL version exercised both cuts with a real owned writer SIGKILL/reap and identified database backend retirement:

1. **COMMIT not forwarded:** after the actual same-fence guardian renewal ACK, the complete tenant snapshot includes every lease field and stays identical across the cut and writer/backend retirement. No closure exists. An ID-only finalizer first returns NotApplicable. A cold process receives only tenant/run, reconstructs the registry from the actual parent admission, whole child account, original child intent and stored graph pins, rereads complete DIRECT including the real cancellation event bytes, waits for natural database-clock expiry of the original lease, then claims F3 and runs the public Loop.
2. **COMMIT response withheld:** the original complete v3 closure is already durable. The cold process uses the ID-only finalizer before constructing an ExecutableGraphRegistry or an application provider and preserves the original closure bytes and historical ACK. Terminal confirmation commits once, then returns the identical event idempotently without another tenant write.

Both retain the original ordinary owner as Executing and its complete request/registration/binding/publication. There is no ordinary owner completion/result, Join consumption or resume callback. Complete priced DIRECT plus the actual settled child's two model turns and one tool call is counted once; new executable application counters remain zero. This proves the selected profile's owned writer/backend retirement, not a general descendant/process-tree owner or the complete R1 failure matrix.

The marker field `registry_or_provider_constructed=false` refers to the executable application registry (ExecutableGraphRegistry) and application provider. The cold finalizer still constructs its JsonSchemaRegistry through `cancellation_schemas()`.

## Evidence and remaining work

The machine record pins both source-specific Root indexes, the actual build, the two 95-file capture manifests, raw stream hashes, original closure/terminal digests and four cut records. Local directed acceptance is `published-join-v3-public-loop-directed-acceptance-root-v2.json`, SHA-256 `5f9a16b827482dc8c7ea9c81278b3e66f9adb32369e8a62c9992d204c6d4d006`. Raw captures have no redactions.

The first candidate build exited 101 at two invalid `json!([0; 6])` expressions. It never ran the case and is not a functional RED. Its frozen source and failed build remain retained; authoring-v2 changed only those JSON expected arrays to six explicit zeros.

The new combined candidate still needs its complete six gates, full Native suites and its own publication/CI identity. Adding this documentation does not extend the local minimal source's identity or transfer the old CI result. Standalone Driver mutation interruption, broader R1/R6/R7 gates, MCP stdio, Supported and release qualification remain unadjudicated/open.
