<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Child-Join logical backup/restore qualification

[中文](child-join-backup-restore-qualification.zh-CN.md)

`child-join-logical-restore-v1` exercises a real PostgreSQL custom-format
`pg_dump` and `pg_restore` across two fresh, isolated databases. It is a
restore-integrity drill for one durable child-run/Join path, not a production
backup policy or point-in-time recovery (PITR) claim.

The source database runs the exact checked migrations and commits an admitted
child, terminal settlement, published Join, fenced physical parent attempt,
pending node result and unique Join consumption. The source store closes before
the backup. `pg_dump` runs inside the pinned PostgreSQL service image; the
archive is streamed in memory, SHA-256-bound for evidence, and restored with
`pg_restore --single-transaction --exit-on-error` into a separate empty
database. The restored runtime connects without running migrations; that
connection must verify the original migration checksums and required schema.

An independent restored-store reader validates the exact source snapshot:
both run projections, immutable ownership/settlement and budget account, Join
head and consumption, physical attempt, pending result, current checksum-bound
checkpoint and both complete journals. It rejects an unrelated tenant's run
lookup. A rebuilt graph registry then resumes the restored parent to a real
non-initial checkpoint: the consumed Join node never executes again and its
successor executes once. The drill drops only its two uniquely named test
databases after a successful run.

## Reproduce

Use a disposable loopback PostgreSQL 16 or 17 Docker container. Set
`STATEKNOT_TEST_POSTGRES_CONTAINER` to that exact container ID or name and
`STATEKNOT_TEST_DATABASE_URL` to its loopback test database. The test refuses
non-loopback database targets and malformed container identifiers.

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_REQUIRE_BACKUP_RESTORE_TESTS=1 \
STATEKNOT_TEST_POSTGRES_CONTAINER=<disposable-container> \
STATEKNOT_TEST_DATABASE_URL=<loopback-test-database-url> \
cargo test -p stateknot-runtime --test postgres --locked -- \
  --exact child_admission::ownership::join::backup_restore::consumed_child_join_survives_isolated_backup_restore \
  --nocapture --test-threads=1
```

Both mandatory PostgreSQL CI jobs require exactly one
`STATEKNOT_CHILD_JOIN_BACKUP_RESTORE_EVIDENCE` record, retain their logs and
source/image inventory for 30 days, and fail if the container identity or
archive process is unavailable. Archive bytes, database URLs, credentials and
Agent payloads are not included in the evidence marker.

## Production gate still open

This test restores a quiesced, single-database logical archive within one
PostgreSQL server. It does not exercise continuous WAL archiving, PITR to a
chosen timestamp, synchronous-standby failover, RPO/RTO, large datasets,
object-store version/key alignment, all durable record families, or
application-wide recovery under load. Those require separate reference
topology drills before StateKnot can claim a production disaster-recovery
profile or overall production readiness.
