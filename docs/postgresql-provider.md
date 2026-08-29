<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# PostgreSQL durability provider

`stateknot-store-postgres` is the first implementation slice of draft
[RFC-0003](rfcs/0003-postgresql-durability-recovery-and-migration.md). It is
pre-alpha and unpublished. This guide records the operational contract already
enforced by code and the remaining blockers that prevent a production release.

## Implemented boundary

The provider currently supplies:

- PostgreSQL 16/17 qualification with exact, checksum-pinned migrations;
- tenant-scoped run admission and validated canonical lifecycle storage;
- RFC 8785 journal payload bytes, stable event-ID idempotency bound to the exact
  lifecycle projection, exact-head optimistic concurrency, and
  payload/intent/projection/event hash verification;
- locked pure `RunTransition` application with the journal fact and projection
  committed in one transaction;
- immutable graph/state checkpoints with exact parent and journal anchors,
  graph/state-schema pins, canonical state bytes, sorted next-ready nodes, and a
  validated current-checkpoint pointer;
- atomic control-plane and worker checkpoint APIs whose event, lifecycle
  projection, checkpoint row, journal head, and checkpoint pointer commit or
  roll back together;
- database-clock lease claim, renewal, release, forced supersession, monotonically
  increasing fencing epochs, and exact worker predicates on both event and run
  head writes;
- bounded repeatable-read journal paging with complete cursors and hash-chain
  verification, plus bounded newest-to-oldest checkpoint-lineage paging whose
  complete cursors remain valid across later barrier commits and whose reads
  validate canonical bytes, redundant columns, parent links, checkpoint
  integrity, and every exact journal-anchor event;
- startup refusal for missing, newer, older, checksum-mismatched, or incomplete
  schema state;
- secure TLS verification by default, bounded pools/timeouts, explicit
  `READ COMMITTED`, transaction-local synchronous commit, and redacted errors.

No transaction remains open across model, tool, remote-agent, or human work.
Stable run, attempt, and event identities must be allocated before calling the
provider so a caller can safely retry an uncertain whole-transaction outcome.

## Deployment sequence

Use separate migration and runtime credentials. Migration is an explicit
deployment action and closes its temporary pool before returning:

```rust,ignore
use stateknot_store_postgres::{PostgresStore, PostgresStoreOptions};

let options = PostgresStoreOptions::default();
PostgresStore::migrate_database(&migration_url, options.clone()).await?;
let store = PostgresStore::connect(&runtime_url, options).await?;
```

The migration role needs database `CONNECT`/`CREATE` and permission to create
`public._sqlx_migrations` and the owned `stateknot` schema. It need not be a
superuser. The runtime role needs `CONNECT`, schema `USAGE`, read access to
`public._sqlx_migrations`, `SELECT`/`INSERT`/`UPDATE` on `stateknot.runs`, and
`SELECT`/`INSERT` on both `stateknot.run_events` and
`stateknot.run_checkpoints`. Do not grant runtime DDL, checkpoint update, or
delete permissions. Exact role/grant SQL will be shipped only with the
role-separated server boundary so this document does not invent
deployment-specific role names.

`PostgresTransportSecurity::VerifyFull` is the default and overrides weaker URL
settings. `RequireEncryption` deliberately forgoes server-identity verification.
`Disabled` is only for a trusted local socket or isolated test network.

## Validation

CI runs 14 integration tests against digest-pinned PostgreSQL 16 and 17 images.
They cover fresh migration, startup refusal, an existing v1 history upgrading to
v2 without guessed projection intent, admission, direct lifecycle transition
enforcement, future-clock rejection, event and projection-intent conflicts,
lost-ack idempotency, bounded journal and reverse-checkpoint-lineage paging,
exact/crossed cursor rejection, continuation across a concurrently advanced
current checkpoint,
renewal/expiry/release/supersession, stale-worker fencing and retry after
takeover, clock-regression rejection after renewal, atomic rollback after
injected event/checkpoint inserts, fail-closed checkpoint and journal-anchor
corruption, a missing parent exactly beyond a page boundary, 100 concurrent
journal appenders, and 24 competing checkpoint writers converging on one
contiguous lineage.

To run the database suite manually, point it at a disposable PostgreSQL instance:

```console
STATEKNOT_TEST_DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DATABASE \
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
cargo test -p stateknot-store-postgres --test postgres --locked
```

The opt-in test role must be allowed to create and drop a temporary database so
the unmigrated-startup path can be tested. Without the URL, local workspace tests
skip external-database cases; CI sets `STATEKNOT_REQUIRE_POSTGRES_TESTS=1` so a
missing service cannot appear green.

Recovery can now call `load_checkpoint_lineage_page` without a cursor and follow
each exact `next_cursor` until the superstep-zero root. The first returned value
is the current barrier observed in the initial repeatable-read snapshot; immutable
continuation cursors remain valid if a later barrier advances the run between
pages. After the lineage validates, recovery pages the journal strictly after
that barrier's trusted journal head with `load_journal_page`. This composes the
durable checkpoint-and-suffix input; the scheduler that replays the suffix and
resumes ready nodes is not implemented yet.

Migration 2 adds nullable `run_events.projection_digest` because migration-1
rows do not contain enough information to reconstruct the caller's projection
intent. Those events remain readable and verifiable. A same-ID mutation retry
against one of them fails closed with `ProjectionIntentConflict` rather than
guessing that the projections match.

## Not yet implemented

This slice is not the complete durable runtime. It does not yet implement
pending node-result writes, node attempts, tool/invocation ledgers, interrupts,
timers, outbox, artifacts, scheduling/readiness queues, automatic corruption
quarantine, retention/archive/legal hold, backup/restore, failover
qualification, or the 10,000-race stale-worker gate.

The current pool is a trusted server-side persistence boundary. Database
credentials must not be distributed to untrusted workers: PostgreSQL
stored-procedure/role separation and the final control-plane/worker service
boundary remain required by RFC-0003. Passing the current integration tests does
not establish an RPO/RTO, multi-replica, security-role, or production-support
claim.
