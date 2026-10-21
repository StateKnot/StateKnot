<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental whole-frame Store qualification

This record qualifies the frozen Core/PostgreSQL increment through whole closure,
not milestone R1 or actual nested Driver execution. [PR #161](https://github.com/StateKnot/StateKnot/pull/161)
and RFC-0022 remain Draft. The [machine-readable evidence](experimental-whole-frames-2026-10-10.json)
retains exact sources, original failures, log digests and bounded scope.

Source `f05d473fcef967bbe1690768562648b8e3cefe3e` has tree
`0b2449e15c4bde35b9fd26a0876ac6a06cc37661`. All thirteen jobs in
[CI 38016667371](https://github.com/StateKnot/StateKnot/actions/runs/38016667371)
and [Supply chain 38016932475](https://github.com/StateKnot/StateKnot/actions/runs/38016932475)
pass. CI merge `0662d16f8e45f7479d39464a9e8e59dc79af7379` has the same tree.

| Exact-source local check | Result |
| --- | --- |
| Workspace | 1,900 passed, zero failed, three explicit ignores |
| Strict Clippy, Rustdoc, format | Passed |
| PostgreSQL 16.15 / 17.11 Store | Each: 10 unit, one Artifact and 173 native passed |
| PostgreSQL 16.15 / 17.11 Runtime | Each: 140 passed, including independent LOGIN Schema 33 role profiles |
| ASan/libFuzzer | Local and immutable CI artifact 11655799106: 2,297 seeds, three targets at 10,000 actual mutations each, original caps |
| Current-reader business restore | Both: all 52 table counts/digests, exact catalog/checksums, 16 closures, 40 callers and four provider bindings |

The unchanged seven-level return regression passes on pinned Rust 1.88 Linux with
its ordinary test stack. Both ordinary and closure completion witnesses remain
fully authenticated; the private completion-restoration Future no longer enlarges
ancestor replay layouts. This corrects the earlier insufficient caller-anchor-only fix.

Earlier complete local Store runs each had 172 passes and one `LeaseExpired`;
subsequent complete serial runs passed with unchanged limits. The first Runtime 16
run had 139 passes and one connection SSLRequest `0x00` failure before a query;
the complete same-source repeat passed. These failures are preserved separately.

The backup corpus was written by `bc3a3f1`; current f05 public readers capture and
verify fresh logical business restores, including original Root/lifecycle and
immutable Existing retries. This does not qualify backup ACLs or historical
N-1/N-2 executables. Earlier schema-only evidence stays tied to its original source.

Claimed scoped planning is a separate development increment and inherits none of
these current-source pass claims. Actual nested registry/Driver dispatch, scoped
application replay, child/Join execution, full process fault coverage, historical
compatibility and production capacity remain open. No Supported declaration,
R1/G3 completion, release or deployment follows from this record.
