<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental scoped attempt base proof

[中文](experimental-scoped-attempt-base-proof-2026-10-15.zh-CN.md) ·
[Machine-readable evidence](experimental-scoped-attempt-base-proof-2026-10-15.json)

A successful ordinary scoped `load_node_attempt` authenticated the same complete
base twice in one repeatable-read transaction. The public read now consumes a
private, non-Clone proof of the first complete authentication. The proof is
issued only after framework Start recognition returns no match, the complete
base/history passes, the actual Start journal anchor passes, and the activation
is non-Root. The proof, Start and complete durable result must name the exact
same activation. Completion bytes and projections, the completion journal
anchor, result bytes, result owner and complete result head, and the actual
tool/model/Join bindings still receive their original checks.

Only this public read supplies the proof inside its own transaction. Mutation,
post-guard and paginated history readers continue through the complete original
path with `None`; Root and recognized framework Starts issue no usable proof.
The change adds no metadata authentication, persistent cache or cross-transaction
reuse. Schema, SQL literals, locks, fences, deadlines, retry/event ceilings and
resource limits remain unchanged.

The first uninstrumented candidate passed Store package checks and selected
Native tests, but full workspace Clippy exposed two `E0275` Send-trait recursion
errors in the Runtime ProviderNative Agent Future. The adopted candidate erases
the complete private `verify_node_attempt_start_with_base` Future behind
`stateknot_core::BoxFuture`. Its transaction borrow is `'a`, its Start borrow is
`'start: 'a`, and the returned proof still borrows that original Start. All 123
original body tokens and the surrounding source remain equal. The complete
`after_start` phase remains unchanged. No recursion limit, thread stack or
resource ceiling was increased. [Original failure](evidence/scoped-attempt-base-proof-2026-10-15/workspace-clippy-before.log),
[body evidence](evidence/scoped-attempt-base-proof-2026-10-15/source-proof.json)
and [independent review](evidence/scoped-attempt-base-proof-2026-10-15/independent-review.json)
are retained; the review reports zero findings for the Future-erasure patch.
The [original patch](evidence/scoped-attempt-base-proof-2026-10-15/erase-only.patch.gz)
and logs with native EOF blank lines use lossless gzip; receipt hashes refer to
the original decompressed bytes, and the manifest also binds compressed bytes.

The final input has 899 files, manifest SHA256
`5cc4e2cd0ab5824b87510dfaea6e95c08e3f1cf0cd20f6e8ebcad0da0b103645`.
The three adopted Rust files match this input, as do 515 files under the crates
and selected build configuration. Qualification documentation was added after
testing and receives no runtime qualification. Formatting, strict full-workspace
all-targets/all-features/locked Clippy, 98 workspace suites with 2,048 passed and
six existing ignored tests, 45 separate doctests, strict Rustdoc and the fixed
`nightly-2026-10-07` workspace check all pass. Database variables were cleared
for these gates; their workspace counts do not establish Native qualification.

After explicit package cleanup, a fresh Store build produced binary SHA256
`8abd33d1dbd7077608a7a3fd16cf77d4beb3b94ecc8bf7261c312e846b2883ad`.
The same six selected Native tests pass on PostgreSQL 16.15 and 17.11, each with
one passed, zero failed and zero ignored. They cover the successful scoped read
and physical corruption rejection, scoped takeover, framework-caller completion
rejection, checksum-valid Return substitution, physical attempt history and
identity, and actual tool/model bindings. Default stacks, original fixture
deadlines and full owning authentication remain in force. These 12 passes do
not replace the complete Native suites.

Earlier instrumented diagnostics on both PostgreSQL versions observed 93 to 53
SQL statements per public read over five trials per version: complete base,
closure, admission and history authentication changed from twice to once;
the actual pending bindings remained checked. Those observations belong to the
separately frozen diagnostic binaries, not the final binary above. Concurrency
and cache effects prevented a latency claim even for those samples. They grant
no final-binary latency, capacity or seven-level recovery qualification.

The older `1fccf1816a2549d8ca7296b635434b52b30140e9`
[CI run](https://github.com/StateKnot/StateKnot/actions/runs/38700608003)
finished with both PostgreSQL jobs cancelled by their original 30-minute job
limit. Store Native tests passed 251/251 in each job and 11 other jobs succeeded;
Runtime and subsequent qualification steps did not finish. This is an incomplete
CI result on different source, not approval of this increment.

Raw logs, immutable source hashes, the original compiler failure, body proof,
review and diagnostic SQL captures are retained in the
[evidence directory](evidence/scoped-attempt-base-proof-2026-10-15/README.md).
Its verifier checks hashes, summaries, body equivalence and adopted inputs.
Complete Store/Runtime Native suites, Linux and original higher-fence/seven-level
requalification, final schema36/scoped ChildJoin composition, the full fault and
compatibility matrices, capacity and release gates remain open. RFC-0022 and R1
remain open; PR #161 remains Draft. This increment grants no Supported or
production-release status.
