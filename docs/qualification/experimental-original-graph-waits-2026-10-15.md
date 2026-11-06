<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Experimental original Graph Waits: exact execution evidence

Date: 2026-10-15. **Exact execution passed; resource cleanup remains open.**
The frozen schema36 + scoped-attempt-proof candidate passed six static gates,
both PostgreSQL majors' complete four Native suites, and separate mandatory
logical restore and PITR cases. The PG16 PITR fixture left one test-owned WAL
volume. Its successful durable assertions do not establish successful cleanup
or complete production qualification.

The [machine profile](experimental-original-graph-waits-2026-10-15.json),
[evidence seal](evidence/experimental-original-graph-waits-2026-10-15/seal.json)
and [verifier](evidence/experimental-original-graph-waits-2026-10-15/verify.py)
bind the results below. [中文对照](experimental-original-graph-waits-2026-10-15.zh-CN.md).

## Exact source and adoption

The tested candidate contains 922 files, tree SHA-256
`88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb`.
Its complete source archive and inventory are retained. The adoption profile
binds a conservative superset of all **530 crate, configuration and fixture
inputs**, including migrations, tests and role SQL. Actual main adoption applied
47 code files and eight separately reviewed documents on base
`a9b265d5b3a96e0e5bbed604b7558f4ae9edc136`; the recorded compiled input bytes
match this exact candidate. New RFC-0024/RFC-0025 documents and existing history
remain. This does **not** assert that the entire main Git tree equals the old
922-file tree. `--workspace` rechecks the actual current files and paths.

Four cache-external Native executables were freshly compiled from this source
and rehashed. Cargo artifact messages show `fresh: false`, debug assertions and
overflow checks retained. Rust 1.88, `-D warnings`, disabled incremental caches
and zero development/test debug symbols were used. Default stack and each
fixture's original leases, transaction limits, pool sizes, quantum and assertions
were retained; this does not mean every fixture uses identical resource values.
No executable or build-cache bytes are included in the compact evidence.

## Observed gates

| Check | Observed result |
| --- | --- |
| Formatting and strict whole-workspace Clippy | exit 0 each |
| Workspace all targets/features, locked | 2,064 passed; six original ignored; zero failed |
| Independent workspace doctests | 45 passed; zero failed |
| Strict Rustdoc | exit 0 |
| Fixed `nightly-2026-10-07`, all targets/features, locked | exit 0 |

Database environment was removed from the static run. Its harness counts are
not Native database execution evidence. Original loopback HTTP tests ran with
the required local permissions; assertions were not removed or weakened.

| Native suite / separate case | PG16 | PG17 |
| --- | --- | --- |
| Store | 264 / 0 / 0 | 264 / 0 / 0 |
| Runtime | 189 / 0 / 0 | 189 / 0 / 0 |
| Integrations | 2 / 0 / 0 | 2 / 0 / 0 |
| Recoverable Agent example | 1 / 0 / 0 | 1 / 0 / 0 |
| Explicit mandatory logical restore | exact 1 / 0 / 0; one marker | exact 1 / 0 / 0; one marker |
| Explicit mandatory PITR | exact 1 / 0 / 0; one marker | exact 1 / 0 / 0; one marker |

Numbers are passed / failed / ignored. The four suites account for 912 libtest
executions; separately forced restore/PITR add four, totaling **916 executions**.
They are not 916 independent fault scenarios. Each major's Runtime189 contains
168 ordinary database scenarios, 15 auxiliary subprocess workers, two independent
resource-gated entries and four harness/protocol self-tests. A worker entry may
return without its controller input; ordinary backup/PITR entries may return
without the separate resource switches. The explicit cases above supply those
switches and validate their unique, complete markers.

The ordinary suites used PostgreSQL 16.15 and 17.11. Both finished with all 36
successful migration rows and 54 business tables. Original process-kill,
commit-loss, restricted SQL-role and maintenance markers are retained and
checked against the exact Runtime inventory. The earlier selected role fixtures
keep their own 919-source identity; the final Runtime also emitted its real
role-profile marker once per major. These bounded fixtures are not a general
permissions, deployment, capacity or performance certificate.

PITR used the same final Runtime executable on a **macOS/aarch64 test process**
and pinned **Linux/arm64 PostgreSQL containers** (16.14 and 17.11). This is not a
Linux-host test or a reused CI x86_64 qualification. Its markers confirm base
backup verification, named restore, exclusion of the late write, complete
durable snapshot, cross-tenant refusal and actual noninitial replay. Logical
restore separately confirms isolated source/restore, schema, complete snapshot,
cross-tenant refusal and noninitial replay. Original profile resources were kept.

## Open resource cleanup

After PG16 PITR, volume
`stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1` remained. The original before/
after inventories show its creation; read-only inspection confirms the volume
exists and has no referencing container, and no container with that UUID remains.
PG17 introduced no additional residual volume. The original Drop does not check
removal errors; the actual removal command's failure stderr/cause was not captured,
so the precise cause remains open. All original evidence and the volume are
preserved; this report did not delete, stop or prune resources.

This is a genuine resource-lifecycle exception, separate from the successful
durable assertions and from the SQL fixture's formatting note below. Complete
PITR cleanup/resource lifecycle is **not qualified**. The resource audit also
records the actual final disk reading and retained inactive task caches; no
cache-cleanup amount or complete absence of residual resources is claimed.

## Preserved failures and fixture provenance

The original 919-file combined candidate ran the complete suites first. Its
Store results were PG16 **261 passed / 3 failed** and PG17 **262 passed / 2 failed**;
Runtime189, integrations2 and example1 passed on both. Those raw failures and
exact follow-ups remain preserved. They are not relabeled as passes of 922.

Only two Store test modules changed from 919, with three provenance fixture files
added. Production/configuration and migrations 1–36 remained exact. The timer
oracle now distinguishes an event committed before the absolute due instant
(`graph_registered`) from one committed after it (`graph_elapsed`); both real
branches and timestamps were observed. The stale source32 fixture was replaced
with original facts exported using an actual schema35 writer and the existing
schema32 reconstruction. This is not a historical schema32 release. The unmodified frozen899 baseline's **514 compiled inputs** equal commit
`83559f…`; the actual producer applies one exact exporter-only test-file
override to that baseline. Its modified test input is not claimed equal to the
commit. The complete Git archive
has **980 files**, while the earlier frozen source has **899**. Their whole trees
are deliberately not described as equal. The archive, exporter, one original
test-file override, all 514 input hashes and actual fixture bytes are retained.

Initial fixture compile/format failures, runner ordering failure, original exact
reds and subsequent greens remain. The separate Return test's original
**2-second lease and `pg_sleep(2.1)`** were not changed; its original full-run setup
failure and exact reruns remain visible, and the final complete Store suites
passed it. The nonempty current-source SOURCE35→36 regression is separately
preserved and is also part of the final Store inventory.

The generated SQL fixture retains its original pg_dump EOF blank line and exact
hash. Applying the patch reported that one formatting warning. The recorded
tracked-only `git diff --check` exit 0 did not include the then-untracked fixture;
no full staged whitespace check is claimed green. No whitespace policy was
relaxed, and other whitespace errors are not exempted.

## One actual released-writer case

The unchanged sealed alpha.1 evidence bundle records the only actual published
release: original registry Store/Core archives and lock, schema26 writer, then
one noninitial Root scenario on each PG major using an experimental **920-input
reader**. Store library/tests/lock were unmodified; five original JSON files from
the published Core package supplied only the sibling paths needed by its tests.
All 39 original rows and their complete original column/bytea values were
preserved. Migration additions and the six actual lease/fence column changes
after claim are listed individually. The expected old-writer/new-schema
rejection remains a negative result, not a Native pass.

The separate proof binds **160 production inputs** and reused parent test helpers
to 922. It does not assert that the alpha case ran on the 922 executable, that
the two whole sources/binaries are equal, or that qualification transfers
automatically. This is one concrete scenario and one released writer, not an
N-2 release or a full compatibility matrix. The nested sealed bundle's own
offline verifier is executed again by this bundle's verifier.

## CI and remaining claim limits

Previous schema35 head `c7d8768…`, run `38717156638`, completed with 11 successful
jobs and two cancelled ordinary PostgreSQL jobs. Both annotations state
“The job has exceeded the maximum execution time of 30m0s”. Those snapshots and
annotations are retained. They are not results for this schema36 candidate.
The local four-suite-plus-backup chains took approximately 3,215/3,179 seconds;
the CI hardware was not reproduced and its 30-minute budget was not changed.
At sealing, no new candidate CI run had been observed. Full CI is not green.

RFC-0022 remains Draft. Full R1, complete fault/compatibility coverage, production
capacity/latency, schema37 Scoped Join and the stdio owner remain open. The
independent A2A request-facts candidate is excluded. Earlier A-only SQL-count
diagnostics do not confer latency or performance qualification on this binary.

Run the following against the final adopted worktree; the default check is pure
offline evidence verification, and `--workspace` additionally checks actual
compiled input paths/bytes without substituting the isolated candidate:

```console
python3 docs/qualification/evidence/experimental-original-graph-waits-2026-10-15/verify.py --require-execution-record --workspace
```

Execution-record completion here means the sealed exact execution record, with the explicit open
cleanup exception. It does not close the remaining production milestones.
