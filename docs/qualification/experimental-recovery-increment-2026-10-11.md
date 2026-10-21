<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental recovery increment: 2026-10-11

[简体中文](experimental-recovery-increment-2026-10-11.zh-CN.md) ·
[Machine-readable evidence](experimental-recovery-increment-2026-10-11.json)

These are separate frozen-source results. R1 remains open and RFC-0022 remains
Draft. They do not qualify a later combined tree or a production release.
The JSON records exact commits, source manifests, commands, exits, log hashes,
database images and actual compilation paths. Raw evidence is retained in the
task-owned paths it lists.

| Source | Actual scope | Result |
| --- | --- | --- |
| `3236208f76b2e1146952507e3ebc92ebe5294dbb` | Full workspace static gates | fmt, strict Clippy/Rustdoc, 1,995 tests and 45 doctests passed; three existing ignored tests. Database entry points without a configured database are not Native evidence. |
| `a21534cf3a18d660c227c5daea4c83d46aff9783` | Scoped child ownership, Join, cancellation and original ancestor limits | PostgreSQL 16.15/17.11: 14 actual tests each passed. |
| `a1bc43c9f2d0d2d03c954f716cc1c35fa69ab641` | MCP reconciliation suite, including real HTTP success/error recovery | PostgreSQL 16.15/17.11: ten tests each passed, including authorization, concurrent receipts and lost HTTP acknowledgement. |
| `01beba98b8e4e0ed6f52238c80abc34dfc57611f` | Complete application replay and deterministic Wait transcript | Seven actual probes per PostgreSQL version passed. Three read-only probes per version retained all 53 business tables and the migration table byte-for-byte. |
| The same `01beba98` source | Full workspace static gates | fmt, strict Clippy/Rustdoc, 1,996 tests and 45 doctests passed; six ignored corpus/other tests. Fixed `nightly-2026-10-07` failed in the original Driver future chain; the original log remains preserved. |
| `bb8a60675049c52619ce96e8c1b8aa3a3fe2f353` | Fresh Driver after Root Continue yield or resolved Root Wait | PostgreSQL 16.15/17.11: two tests each passed. Higher fences replayed the complete original disposition and executed each node once; original Wait ACK recovery also passed. |

Application replay still authenticates complete owning records. The prefix
proof is additive; this report makes no linear-history-cost claim. Unprovable
legacy Wait, Terminal or changed generic projections remain operationally
unavailable without quarantine. UTC, ISO DateStyle and hex `bytea_output` make
the complete Wait transcript independent of connection rendering settings.

The first Root recovery differential run archived two different sources before
building them in one target directory. Cargo reused the original red binary
for the candidate run. That candidate verdict is invalidated. Its logs remain
preserved; the replacement used an initially empty target, compiled the exact
candidate source, checked every source hash before/after, and proved that the
DCO commit has identical tracked files. Other listed first-build logs were
separately checked for their actual source paths. Reusing a binary across
PostgreSQL versions is valid only for the same verified source.

Ordinary checks kept the default thread stack and used
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`.
No lease, deadline, timeout or recursion limit was increased. Disk checks
preceded builds. Logs and receipts remain outside build caches; no source,
database, Git history or unconfirmed cache was deleted.

The next qualification must cover the final combined source, scoped child
Join in the actual Driver, ordinary Root compatibility, elapsed absolute Waits,
each-write process/COMMIT/ACK faults, fixed nightly/Linux, real historical
N-1/N-2 programs, CI and the R6/R7 reference deployment requirements.
