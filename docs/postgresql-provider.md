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
- immutable tool- and model-invocation intents and hash-linked revisions with
  exact base-checkpoint and journal anchors, stable logical versus physical
  attempt identities, bounded ascending history verification, closed tool
  prepared/executing/committed/failed/unknown outcomes, and closed model
  prepared/executing/committed/failed outcomes;
- a run-wide tool/model physical-attempt registry whose primary key prevents
  cross-ledger reuse and whose deferred exact-kind/invocation/revision foreign
  keys make each `StartAttempt` claim inseparable from its immutable revision;
- atomic fenced prepare/advance APIs whose event, invocation revision, current
  invocation pointer, and run journal head commit or roll back together, with
  exact lost-ack convergence and no blind retry from an unknown outcome;
- successor-checkpoint rejection while any invocation rooted at the exact
  current checkpoint remains prepared, executing, failed, or unknown;
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
`stateknot.run_checkpoints`, `SELECT`/`INSERT` on
`stateknot.tool_invocations` and `stateknot.model_invocations` plus
column-scoped `UPDATE` only for
`current_revision`, `current_status`, `current_attempt_id`,
`current_record_digest`, and `updated_at`, and `SELECT`/`INSERT` on
`stateknot.tool_invocation_revisions`,
`stateknot.model_invocation_revisions`, and `stateknot.run_attempt_claims`. Do
not grant runtime DDL, checkpoint or invocation-revision update, or delete
permissions. Exact role/grant SQL will be shipped only with the role-separated
server boundary so this document does not invent deployment-specific role
names.

`PostgresTransportSecurity::VerifyFull` is the default and overrides weaker URL
settings. `RequireEncryption` deliberately forgoes server-identity verification.
`Disabled` is only for a trusted local socket or isolated test network.

## Validation

CI runs 27 integration tests against digest-pinned PostgreSQL 16 and 17 images.
They cover fresh migration, startup refusal, an existing v1 history upgrading to
v4 without guessed projection intent, real v3 tool-attempt history
backfilled into the v4 run-wide registry, admission, direct lifecycle
transition enforcement, future-clock rejection, event and projection-intent
conflicts, lost-ack idempotency, bounded journal and
reverse-checkpoint-lineage paging, exact/crossed cursor rejection,
continuation across a concurrently advanced current checkpoint,
renewal/expiry/release/supersession, stale-worker fencing and retry after
takeover, clock-regression rejection after renewal, atomic rollback after
injected event/checkpoint/invocation writes, fail-closed checkpoint,
invocation-byte, journal-anchor, and projection-binding corruption, a missing
parent exactly beyond a page boundary, 100 concurrent journal appenders, 24
competing checkpoint writers converging on one contiguous lineage, and 24
competing tool and model invocation advances admitting exactly one physical
attempt. Model coverage additionally proves delayed retry, cross-tool/model
attempt rejection, exact response provenance, and rollback of an event,
revision, current pointer, and attempt claim as one unit. Invalid non-ready and
nested-namespace activations leave no event or invocation row; cancellation
blocks new tool/model work while preserving already executing outcome evidence.
A checkpoint cannot advance past a prepared or otherwise unsettled invocation,
and a committed result releases that guard.

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

Tool recovery loads the current record with `load_tool_invocation` or follows
`load_tool_invocation_history_page` from revision zero using its exact full-record
cursor. Both APIs revalidate canonical intent/record bytes, redundant SQL
projections, the base checkpoint, and every restored record's exact journal
event plus projection digest; the history API additionally replays every
hash-linked transition. A `Prepared` record may claim one physical attempt;
`Unknown` can only enter an explicit reconciliation transition, never a blind
execution retry. The current checkpoint format carries a root-graph
`ReadyNodes` set, so this provider accepts only root-namespace activations whose
node is present in that exact set; nested activations fail closed until a
namespaced checkpoint-ready contract is implemented. New preparation and
`StartAttempt` commits require an `Active` run. Result, error, and reconciliation
commits may also finish while the run is `Waiting` or
`CancellationRequested`; terminal runs reject new worker mutations, while an
exact already-committed event retry still converges. Checkpoint append holds the
same run lock and rejects an exact current checkpoint that still owns a
`Prepared`, `Executing`, `Failed`, or `Unknown` invocation. `Committed` releases
this orphan-prevention guard. It does not yet prove that a node update consumed
the tool result; that stronger barrier contract depends on the pending
node-result ledger listed below.

Model recovery uses `load_model_invocation` and one-record
`load_model_invocation_history_page` pages. The immutable intent stores the
exact negotiated descriptor and request once, while each compact revision is
rejoined with that verified intent and checked against its SQL projection,
predecessor, journal event, and response/error provenance. A real provider
exchange starts only after a fresh run-wide `AttemptId` reaches `Executing`.
Complete responses and public-safe errors may finish while the run is active,
waiting, or cancellation-requested. A failed invocation can dispatch another
attempt only after explicit `SafeAfter` advice and the database-recorded journal
clock prove the delay; hidden SDK retries are outside this contract and must be
disabled.

Migration 2 adds nullable `run_events.projection_digest` because migration-1
rows do not contain enough information to reconstruct the caller's projection
intent. Those events remain readable and verifiable. A same-ID mutation retry
against one of them fails closed with `ProjectionIntentConflict` rather than
guessing that the projections match.

Migration 3 adds `tool_invocations` and `tool_invocation_revisions`. The former
owns the immutable intent and exact current pointer; the latter owns canonical
full records, predecessor digests, journal anchors, and globally run-unique
physical attempt claims. A deferred exact-current foreign key permits revision
zero and its intent to commit atomically without exposing a dangling pointer. A
partial index over non-committed current rows serves the checkpoint guard
without retaining committed rows in the index.

Migration 4 adds `run_attempt_claims`, `model_invocations`, and
`model_invocation_revisions`. It first backfills every v3 tool `StartAttempt`,
then installs deferred exact-kind/invocation/revision claim foreign keys before
model work is admitted. Model intents are stored once; compact revision bytes
retain an intent digest and are rejoined through the core integrity validator.
The migration is checksum-pinned and the v3 backfill is exercised on both
PostgreSQL 16 and 17.

## Not yet implemented

This slice is not the complete durable runtime. It does not yet implement
pending node-result writes, node execution-attempt records, interrupts, timers,
outbox, artifacts, scheduling/readiness queues, automatic corruption quarantine,
retention/archive/legal hold, backup/restore, failover qualification, or the
10,000-race stale-worker gate.

The current pool is a trusted server-side persistence boundary. Database
credentials must not be distributed to untrusted workers: PostgreSQL
stored-procedure/role separation and the final control-plane/worker service
boundary remain required by RFC-0003. Passing the current integration tests does
not establish an RPO/RTO, multi-replica, security-role, or production-support
claim.
