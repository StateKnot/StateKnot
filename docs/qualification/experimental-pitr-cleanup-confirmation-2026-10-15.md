<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Experimental PITR cleanup confirmation

2026-10-15. The independent test-only candidate passed targeted Runtime strict
Clippy, a fresh build, and one mandatory named PITR case on each pinned
PostgreSQL16.14/17.11 image. Each actual execution passed1/0/0, emitted one
complete marker with `resource_cleanup_verified=true`, and left none of its
new UUID's two named containers or three volumes. [中文](experimental-pitr-cleanup-confirmation-2026-10-15.zh-CN.md).

The tested source has922 files, tree
`82fc93f1e66fd13703bd9ae8bff268460829324cebe19ee94eaca497d87aed7d`.
Relative to the [schema36 source](experimental-original-graph-waits-2026-10-15.md),
only `child_join_pitr.rs` changed; production Runtime/Core/Store/SQL are identical.
The fresh saved Runtime binary is
`994be9d00fe567660a847627c82b96a5e51c45e9ded56b0ba54e6760b75f0a32`.
Its189-entry inventory matches the parent; this execution ran only the exact
PITR entry per major, not the full189 suite.

Explicit cleanup now confirms exact named-resource absence before the success
marker under the original30s/10s budgets. Original durable assertions, default
stack/resources and best-effort Drop remain. This proves two successful cleanup
paths; panic, timeout, Docker/host failure and every other failure path remain
unqualified. The old PG16 WAL volume
`stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1` remains untouched, and the old
schema36 cleanup-open record is retained. macOS/aarch64 test processes with
Linux/arm64 database containers do not establish Linux-host qualification.

The [original EN/CN report and verifier](evidence/experimental-pitr-cleanup-confirmation-2026-10-15/README.md)
and [machine profile](experimental-pitr-cleanup-confirmation-2026-10-15.json)
retain all121 original evidence files and their exact hashes; the original seal
is unchanged. `verify.py` is completely offline and runs no compilation or resource
operation. No new archive, binary or build cache was added by this publication.

The [A2A record](experimental-a2a-request-facts-2026-10-15.md) has its separate531-input
binding. A composed main containing both changes still requires its own original
**opt0 six workspace gates; those results are pending here**. Neither record
transfers full Runtime189/four-suite Native, complete CI, failure-path cleanup,
capacity/performance or R1 acceptance to that composition. RFC-0022 remains Draft.
