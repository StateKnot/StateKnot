<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Child-Join physical PITR qualification

[中文](child-join-pitr-qualification.zh-CN.md)

`child-join-named-pitr-v1` is a real PostgreSQL 16/17 physical recovery drill
for one consumed durable child Join. It is narrower than the v1 disaster
recovery acceptance profile: it does not establish an operational backup
service, a time-based RPO/RTO, or zero-loss primary failover.

The test starts a disposable source cluster with continuous WAL archiving to a
private Docker volume. It migrates the exact StateKnot schema and commits child
admission, terminal settlement, published and consumed Join, parent physical
attempt, pending result, journal and checkpoint. It captures the full durable
snapshot, makes a `pg_basebackup` with streamed WAL, and runs
`pg_verifybackup` against its manifest. Only after backup completion does it
create a named restore point and commit a deliberately late write. It switches
WAL and waits for the segment containing that write to reach the archive.

The source process is stopped. A different PostgreSQL container starts from
the physical base backup, using the archived WAL and a `recovery.signal` to
promote at the named restore point. Its schema is connected without migration
and must pass exact migration checksum verification. The late write must be
absent. An independent store reader compares both run projections, ownership,
settlement, budget, Join, attempt, pending result, checkpoint and full journals
against the pre-backup snapshot. Cross-tenant lookup is refused; a rebuilt
graph registry then replays a non-initial parent checkpoint without executing
the consumed Join node again. The test uses unique, private Docker containers
and volumes, which it cleans up after completion.

## Reproduce

Use a digest-pinned PostgreSQL 16 or 17 image. Docker must be available, and
the test opens only ephemeral loopback TCP ports. No existing database is
modified.

```sh
STATEKNOT_REQUIRE_PITR_TESTS=1 \
STATEKNOT_PITR_POSTGRES_IMAGE=postgres@sha256:<reviewed-image-digest> \
cargo test -p stateknot-runtime --test postgres --locked -- \
  --exact child_admission::ownership::join::pitr::consumed_child_join_survives_named_point_in_time_recovery \
  --nocapture --test-threads=1
```

Dedicated PostgreSQL 16 and 17 CI jobs require exactly one
`STATEKNOT_CHILD_JOIN_PITR_EVIDENCE` record each and retain it with the
commit/tree, lockfile, toolchain, image and host inventory for 30 days. The
evidence marker contains no backup bytes, credentials or Agent payloads.

## Remaining production gates

This is one small, disposable cluster and one child-Join record family path.
It does not test timestamp selection, a continuous backup service, missing or
corrupt WAL, backup/key/object-store consistency, every durable record family,
retention/legal hold, reference-sized datasets, synchronous-standby promotion,
acknowledged-write RPO 0, measured RTO, multi-role application restart, or
long soak. The full [v1 scenario](scenarios/002-long-running-approval.md) and
[RFC-0003](rfcs/0003-postgresql-durability-recovery-and-migration.md) remain
open. Operators must not use this test's ephemeral Docker volume as a backup
plan.
