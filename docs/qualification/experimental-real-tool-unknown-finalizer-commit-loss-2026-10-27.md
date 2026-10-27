<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Real Tool Unknown followed by parent finalizer COMMIT loss

[中文](experimental-real-tool-unknown-finalizer-commit-loss-2026-10-27.zh-CN.md)

Status: exact isolated local evidence, with the new integration composition pending validation. This adds a test-only characterization after the [original real-Unknown accounting case](experimental-failure-close-real-tool-unknown-2026-10-21.md). It adds no production interface, implementation, schema, migration or business backend, and does not accept RFC-0022 or close R1/R6/R7.

## Executed source and retained receipts

The executed frozen source is r1-real-unknown-finalizer-fault-source-v3: 977 files, canonical SHA-256 ce3b2c6c43fa654c3174f1badc08cd0e9719f7d992614b48347be7d41cf1563b; its 564 compiled inputs hash to 9b4854020bb8e3bfd54ebc54fd6e4c807ab8b8f83ae94e6e05e5e61c9bba1829. These identify the existing isolated execution, not this later composition.

Retained local records under .local-verification/2026-10-18 are:

| Record | SHA-256 |
| --- | --- |
| r1-real-unknown-finalizer-fault-exact-local-acceptance-root-v1.json | 113988dfba8770d9d60c2348787aea25784b39c9c6fee0f390328615200b9137 |
| r1-real-unknown-finalizer-fault-independent-data-review-v1.json | fb0004a7e2607c26857a80abfe70f55b74275fdfb5ad03663a523018a09e5047 |
| r1-real-unknown-finalizer-fault-native-build-v1/manifest.json | bbe18cc5ffc1ead60d3c90767076399484774bd93520d119957fb3460a872ce0 |
| r1-real-unknown-finalizer-fault-pg16-v1/manifest.json | 276cdb4e9430f4f8dd08ac37d6bcaf419d5754bf1f3a56e187291e13273b2592 |
| r1-real-unknown-finalizer-fault-pg17-v1/manifest.json | e21b63a9f94c320e618891edf3eb5d06532b71830f24db1da54b1801d252237e |

The default-feature two-role Native no-run build exited 0. Core, Store and Runtime libraries and the Store test binary were fresh cached artifacts; only the Runtime test binary rebuilt. Actual inventories are Store 276 and Runtime 231. This was not a cold build or full Native test execution. The same frozen source passed strict workspace/all-targets/all-features Clippy (exit 0); the earlier manual_assert Clippy 101 remains retained.

The exact controller child_admission::ownership::failure_close::real_unknown_finalizer_faults::real_tool_unknown_finalizer_commit_loss_survives_kill_and_cold_owning_reads passed once on each PostgreSQL major, with 230 filtered tests and no failures/ignored/measured cases. The macOS aarch64 host used PostgreSQL containers with fsync, synchronous_commit and full_page_writes on.

| PostgreSQL | libtest seconds | Capture seconds | Writer cuts | Ordered preparation checks |
| --- | ---: | ---: | ---: | ---: |
| 16 | 29.17 | 29.190913 | 2 | 36 true |
| 17 | 13.34 | 13.367272 | 2 | 36 true |

## Agreed seam and asserted behavior

The fixture first produces a real Tool Unknown with ReconcileFirst, persists the original parent failure and child cancellation, rejects incomplete parent accounting without a tenant write, denies an unconsented original-operation probe, and explicitly authorizes reconciliation against the original invocation/schema/operation. It compares the complete original receipt and replay ACK and asserts one business send. The child then terminates through its public Loop and settles USD11 before the finalizer subprocess starts. The provider receipt backend remains in the controller; it is not restarted.

The two cuts target parent FailureCloseCompletion reached through public tick. BeforeCommit never forwards COMMIT. CommitResponse observes PostgreSQL COMMIT CommandComplete and idle ReadyForQuery, then withholds both frames from the writer. Each writer is actually SIGKILLed and reaped: four writer kills across the two majors. Before must leave the whole tenant unchanged; After permits exactly the parent terminal delta and one parent journal event. The controller waits for the actual backend disconnect.

A new process receives only tenant/run identity, constructs a Store and calls public tick without a registered provider. Before yields one completed item; After yields zero. The After empty report is not an ACK. Public owning reads compare the original complete parent journal event, run head, failure-close record and child receipt. The complete account equals the pre-terminal account.observe_direct(original terminal ACK, original frozen DIRECT), preserving costs[] and the exact child/delegated settlement, added once. Both the cold worker and controller repeat tick and complete owning reads with whole-tenant no-write assertions. The marker carries event/account digests; complete typed equality is asserted by the executed Rust source.

## Integration and qualification limits

The integration payload copies the formatted, manual_assert-repaired v3 bytes: only a module registration in failure_close.rs, two pub(super) test helpers in failure_close_completion_commit_loss.rs, and the new characterization module. Its new module SHA-256 is 6eb46db851b808d7abe48c4a7bae5b8ba5ebe0036d41d0f4c621c8b5b1399e2a. Production source remains unchanged.

Current CI inventory guards become Store 276 / Runtime 231. Both Runtime lane counts continue to derive from the sorted complete actual inventory: 116/115 passed and 115/116 filtered, with disjoint once-only union. Current Return/Wait exact marker summaries become 1/230. No default feature, debug assertion, overflow check, default stack, lease30, READY90, pair1800, Native30min, owner protocol or legacy evidence policy changes.

The new local-ce3b-runtime-inventory.stdout unit fixture is the original 24,387-byte local build list, SHA-256 ba9106863109842a771ff57f1bf1af7fbba476be72da70118c79f59355a14703. current-provenance.json separates its macOS source/build identity from the retained c9 Linux-CI Store list. Both original c9 lists and their historical provenance remain unchanged; the historical Runtime list still contains 229 tests. Artifact-shaped unit receipts use an explicitly synthetic common source binding and do not relabel either list's origin as a new Linux CI run.

The b5144d44187cec87dbfecf1cf1b5309974267071-based integration must establish its own source/compiled identity, six static gates, 60 shard codec/unit controls, current artifact controls, actual default-feature inventories, full dual-PostgreSQL Native execution and exact-head Linux CI. No future hash or result is prefilled. Old CI and the isolated source's directed passes do not qualify the new composition.

Unresolved Unknown across process loss, provider-backend durability, this profile's Store Existing replay, independent Driver mutation interruption, cross-OS process kill and the broader effect/fault matrix remain open. R1/R6/R7, RFC acceptance, MCP stdio, Supported status and release qualification remain open. Earlier source-specific evidence and failures retain their original scope.
