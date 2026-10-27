<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental nested Driver combination: 2026-10-11

[简体中文](experimental-nested-driver-combination-2026-10-11.zh-CN.md) ·
[Machine-readable evidence](experimental-nested-driver-combination-2026-10-11.json)

These results bind commit `f9cae2db2efc42bb7a399b2428418a3a59858304`, tree
`c239c48fe2c22f331aba629789f249ee63c0b6f3`, and all 880 tracked files.
R1 remains open and RFC-0022 remains Draft. The complete Native CI is failing.
Later changes require their own qualification.

| Actual scope | Result |
| --- | --- |
| Rust 1.88 workspace static gates | Formatting, strict all-targets/all-features locked Clippy, 98 test suites / 2,020 passed / six ignored, 45 independent doctests, and strict Rustdoc passed. Database entry points that return without a database are not Native evidence. |
| Fixed `nightly-2026-10-07` | Strict complete workspace/all-targets/all-features check passed on this exact source. The earlier `01beba98` Driver future failure remains preserved. |
| Local fixed ASan | 314 readers, 71 enum types / 298 variants, 2,304 seeds including seven original terminal-input seeds. All three targets replayed their seeds and completed 10,000 actual mutations each; peak RSS 498 / 820 / 470 MiB. |
| CI fixed ASan | Actual checkout was synthetic merge `ff5584c201799660ed3fb11e81291eff9b50c4ac`, whose parents, tree and complete source hash match the frozen f9 source. Downloaded report and raw target hashes were verified. Each target completed 10,000 actual mutations; peak RSS 456 / 1,026 / 271 MiB. |
| Complete trusted SQL role test | One actual original full test passed on each of PostgreSQL 16.15 and 17.11, schema 34, default stack. It covers separate non-superuser LOGIN roles, ACL/drift negatives, child Join, submission/provider recovery, 24-way races, scoped Start/Barrier/Rebind/Return/Wait/Closure, isolated retention, populated reapply and exact catalog. The original default-stack overflow is resolved in this combination. |
| Complete CI run 38079695038 | Eleven jobs passed. Mandatory PostgreSQL 16/17 Store runs each reported 240 passed / ten failed. Runtime Native and later steps were not reached. |

The Native failures remain explicit: seven fresh INITIAL Root source-parent
cases, two full public Run reads after deliberately corrupting checkpoint
canonical bytes, and a seven-level full application replay that outlived its
original lease. Their original logs are retained. Positive-source Root support,
appropriate corruption assertions and complete owning replay cost are being
addressed separately; no metadata shortcut or lease increase is qualified here.

The first local ASan invocation failed before fuzzing because the existing pinned
`cargo-fuzz 0.13.2` directory was absent from PATH. Its log and prerequisite
report remain. The replacement changed PATH only and executed the full original
qualification on unchanged source. Input size 131,072 bytes, 10,000 mutations,
60 seconds per target, ten seconds per input, 2,048 MiB RSS, two build jobs,
AddressSanitizer and `cfg_fuzzing=false` are unchanged. Both lockfiles and every
source file were checked before and after.

The local checkout contains tracked source only. Static and fuzz targets were
initially empty; actual compilation paths and the role binary digest are
recorded. Ordinary checks used the default thread stack and
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`.
Disk checks preceded compilation; no stack, lease, deadline or timeout was
increased. Logs and receipts remain outside caches. The machine-readable report
records all retained paths, command exits and hashes.

This evidence does not qualify schema 35, cancellation-guardian changes, scoped
Driver ChildJoin, elapsed Waits, the full Native matrix, historical N-1/N-2
programs, R1 or R6/R7. It does not promote the experimental API to Supported or
RFC-0022 to Accepted.
