<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0003: PostgreSQL durability, recovery, and migration

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-08-29
- Tracking issue: Not yet created
- Supersedes: None
- Superseded by: None

## Summary

StateKnot v1 uses PostgreSQL 16 or later as the only production durability
engine. Each run has one append-only, tenant-scoped journal; materialized run
state, checkpoints, invocation records, interrupts, timers, and outbox records
are projections committed atomically with journal facts. A stable `EventId`
provides append idempotency, an exact head prevents lost updates, and a
database-allocated monotonically increasing epoch fences late worker writes.

The journal payload is the RFC 8785 canonical byte form of the closed
`{schema, kind, data}` envelope. Each record binds a payload digest, a complete
intent digest, and a chain digest. The hash chain detects corruption, omissions,
and reordering, but is not represented as proof against an administrator who
can rewrite the entire database.

This RFC separates three kinds of state:

1. protocol-neutral business lifecycle (`RunLifecycle`);
2. append-only durable facts (`JournalEvent`);
3. transient execution ownership (`RunLease` and `RunFence`).

A worker crash or lease change never fabricates a business transition. A
business transition exists only after its journal event and materialized
projection commit in one database transaction.

## Motivation

Agent execution includes nondeterministic model responses, external tool
effects, long waits, and callbacks. Re-running process memory after a crash
cannot prove which effects happened. StateKnot therefore records externally
observed results and decisions before later execution depends on them.

The design follows four independently useful production properties:

- Temporal describes workflow history as an
  [append-only event log](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/workflow/workflow-execution/event.mdx)
  used for recovery and audit;
- Restate makes journal append the durable step commit point and uses
  [monotonic epochs to reject superseded attempts](https://docs.restate.dev/references/architecture);
- PostgreSQL documents that `SELECT ... FOR UPDATE` prevents concurrent writers
  until transaction end and that locks must be acquired consistently to avoid
  deadlocks in its
  [explicit locking semantics](https://www.postgresql.org/docs/current/explicit-locking.html);
- RFC 8785 defines a cross-implementation
  [JSON Canonicalization Scheme](https://www.rfc-editor.org/rfc/rfc8785)
  suitable for integrity operations.

These sources inform the invariants, but StateKnot owns its wire contracts,
status model, SQL schema, and operational guarantees.

## Goals and non-goals

### Goals

- make acknowledged events and projections recoverable with RPO 0 in the
  qualified synchronous-replication configuration;
- serialize writes per tenant/run without holding a transaction open while a
  model, tool, human, or remote agent is running;
- reject every write from an expired or superseded worker at the database row;
- make append retries idempotent without silently accepting conflicting reuse;
- retain exact canonical payload bytes and independently verifiable integrity
  metadata;
- rebuild or validate materialized state from a checkpoint and journal suffix;
- quarantine corruption or unsupported schema versions instead of executing
  best-effort interpretations;
- support rolling expand/contract migrations and N-1/N-2 durable fixture
  compatibility;
- define retention, archival, legal-hold, backup, restore, and observability
  boundaries before the schema is deployed.

### Non-goals

- promising exactly-once behavior for an external system that does not provide
  idempotency, status lookup, or a transaction shared with StateKnot;
- using PostgreSQL advisory locks as durable lease ownership;
- holding SQL transactions or connections across network calls or user waits;
- treating JSONB's normalized representation as integrity bytes;
- making the unkeyed journal hash chain tamper-proof against database
  administrators;
- supporting SQLite, an in-memory production store, Restate, or Temporal as a
  v1 durability backend;
- defining graph/reducer semantics, which belong to RFC-0002;
- defining protocol identity and authorization mapping, which belongs to
  RFC-0004.

## Implemented core contracts

The provider- and database-neutral `stateknot-core` crate now contains:

- `CanonicalJson`: explicit RFC 8785 bytes over resource-bounded JSON, with
  integers outside the interoperable I-JSON safe range rejected before a
  serializer can round them;
- `FencingEpoch`: positive decimal `1..=9223372036854775807`;
- `RunFence`: tenant, run, physical attempt, and fencing epoch;
- `RunLease`: acquisition, latest renewal, exclusive expiry, renewal, and
  supersession validation;
- `JournalSequence`: one-based contiguous decimal sequence bounded to signed
  PostgreSQL `BIGINT`;
- `JournalPayload`: pinned schema, lower-kebab event kind, bounded data, and
  reproducible RFC 8785 digest;
- `JournalEventIntent`: stable event identity, trusted source, payload, and
  domain-separated intent digest;
- `JournalExpectation` and `JournalAppend`: empty or exact-head optimistic
  precondition paired with one idempotent intent;
- `JournalEvent`: committed metadata, payload/intent/predecessor digests, and a
  domain-separated event digest;
- `JournalChainVerifier`: streaming validation for a complete history or a
  suffix after a trusted checkpoint/archive head;
- `Superstep`, `NodeId`, and `ReadyNodes`: bounded deterministic barrier and
  scheduling identities compatible with PostgreSQL constraints;
- `GraphReference`: capability identity plus compiled-graph and state-schema
  digests pinned to one run;
- `CheckpointState`, `CheckpointWrite`, and `Checkpoint`: bounded RFC 8785 state,
  exact parent/journal anchors, graph/schema continuity, idempotent intent, and
  complete checkpoint integrity validation.

These types validate intrinsic values and are used in model/fault tests. They do
not replace database authorization or atomicity.

## User-facing store shape

The current concrete PostgreSQL store preserves separate control-plane and worker
entry points. A future provider-neutral trait will not be published until a
second implementation or runtime boundary proves its shape:

```rust,ignore
async fn append_control_plane(
    &self,
    append: JournalAppend,
    projection: RunProjection,
) -> Result<AppendOutcome, StoreError>;

async fn append_worker(
    &self,
    append: JournalAppend,
    projection: RunProjection,
) -> Result<AppendOutcome, StoreError>;

async fn append_control_plane_checkpoint(
    &self,
    append: JournalAppend,
    projection: RunProjection,
    checkpoint: CheckpointWrite,
) -> Result<CheckpointCommitOutcome, StoreError>;

async fn append_worker_checkpoint(
    &self,
    append: JournalAppend,
    projection: RunProjection,
    checkpoint: CheckpointWrite,
) -> Result<CheckpointCommitOutcome, StoreError>;

async fn load_current_checkpoint(
    &self,
    tenant_id: &TenantId,
    run_id: RunId,
) -> Result<Option<Checkpoint>, StoreError>;
```

`append_worker` rejects an append without a worker source. It never accepts a
caller-selected `ControlPlane` marker. `append_control_plane` is reachable only
after the authenticated API/scheduler/migration authorization path and rejects
a worker marker. This API separation prevents a stale worker from changing one
enum value to bypass fencing.

An append result has three distinct outcomes:

- `Committed(event)`: a new event and all projections committed;
- `Idempotent(event)`: the same event ID already committed with an identical
  event and lifecycle-projection intent;
- error: conflicting event ID, stale head, stale/expired fence, invalid
  transition, corruption, unsupported schema, unavailable database, or another
  explicit store category.

Callers never infer success from a transport timeout. They retry with the same
event ID and identical intent.

## Canonical payload and integrity semantics

### Payload envelope

The exact semantic payload is:

```json
{
  "schema": {
    "id": "https://stateknot.github.io/schema/run-event/1.0.0",
    "version": "1.0.0",
    "digest": "sha256:..."
  },
  "kind": "run-transition-applied",
  "data": {}
}
```

The trusted local registry verifies `data` against the digest-pinned schema
before append. Unknown major versions are retained for audit but cannot drive
execution. Canonical payload bytes are produced once using RFC 8785 and stored
as bytes; they are not reconstructed from JSONB for integrity verification.

JSON integers that would be rounded by an ECMAScript/IEEE-754 implementation
are rejected. StateKnot's exact counters, money, timestamps, UUIDs, and digests
already use their specified string forms.

### Three digests

1. `payload_digest = SHA-256(JCS(payload envelope))`.
2. `intent_digest = SHA-256("stateknot-journal-intent-v1\0" ||
   JCS(tenant, run, event ID, source/fence, payload digest))`.
3. `event_digest = SHA-256("stateknot-journal-event-v1\0" ||
   JCS(tenant, run, sequence, event ID, recorded time, payload digest,
   intent digest, previous event digest))`.

The literal domains and field shapes are versioned wire contract. Digest inputs
are covered by versioned fixtures. A first event has sequence one and no
predecessor. Every later event contains the exact previous event digest.

The chain detects accidental or partial corruption. Authenticity against a
privileged attacker requires independently protected signatures, immutable
exports, or external transparency anchoring and is not claimed by v1.

## Append transaction

All mutation paths use a short transaction. Network calls and execution happen
outside it.

For one requested append the store performs this order:

1. begin a transaction with bounded `lock_timeout` and `statement_timeout`;
2. lock the composite `(tenant_id, run_id)` row with `SELECT ... FOR UPDATE`;
3. look up `(tenant_id, run_id, event_id)`:
   - if found and `intent_digest`, `projection_digest`, and immutable intent
     fields match, return the existing record idempotently after commit/rollback
     of the read transaction;
   - if the event intent differs, return `EventIdConflict`; if only the
     projection intent differs or is unknowable for a migration-1 event, return
     `ProjectionIntentConflict`;
4. compare the complete current journal head with `JournalExpectation`;
5. for worker writes, compare tenant, run, attempt, epoch, and
   `db_now < lease_expires_at` on the locked row;
6. validate the expected run/checkpoint/invocation revision needed by the
   projection;
7. apply the pure state transition and validate resulting domain records;
8. choose `recorded_at = max(database_clock, previous_recorded_at)` so wall-clock
   correction cannot make durable time regress; sequence remains authoritative;
9. construct and insert the canonical journal event;
10. for a checkpoint append, compare its ID/intent and exact parent before stale
    head/fence rejection, then insert the immutable checkpoint anchored to the
    event created in step 9;
11. update the run head and every related projection/checkpoint/outbox row;
12. commit; only then acknowledge the event.

The idempotency lookup intentionally precedes head/fence rejection. A worker
whose acknowledgement was lost may retrieve the event it already committed
even after its lease was superseded. It cannot append anything new.

`READ COMMITTED` plus a row lock is sufficient for single-run serialization.
Transactions touching multiple runs acquire composite keys in deterministic
byte order. Deadlock, serialization, connection, and failover errors are
retryable only at the whole transaction boundary with the same event ID.

## Lease and fencing semantics

### Claim

The scheduler discovers candidates with an indexed readiness query. It may use
`FOR UPDATE SKIP LOCKED` only for queue-like work distribution; a skipped row is
not interpreted as absent. Under the selected run lock, a claim:

- verifies that business state is runnable and no unexpired lease blocks it;
- allocates a new UUIDv7 `AttemptId` before dispatch;
- checks epoch exhaustion, then increments the persisted epoch exactly once;
- records acquisition/renewal time and exclusive expiry from the database clock;
- returns the exact `RunFence` only after commit.

Epoch never resets after release, waiting, cancellation, or process restart.
The first issued epoch is one. A run at maximum epoch is quarantined for
operator repair rather than wrapping.

### Renewal

Renewal conditionally updates the row only when tenant, run, attempt, and epoch
all match and the database clock is strictly before old expiry. The new expiry
must strictly extend the old expiry. A renewal at expiry is rejected. Retry with
the same desired expiry is idempotently observable from the current row but may
not shorten it.

### Release and supersession

An orderly release clears active lease timestamps/attempt only under the exact
fence and retains the last epoch. A new claim increments it. An authorized drain
or repair can revoke an unexpired lease by committing a successor epoch under
the run lock; the old process is fenced immediately even if its local timer has
not expired.

Every worker-originated event, checkpoint, pending write, invocation mutation,
and outbox enqueue includes the same fence predicate in its commit statement.
Checking a token only in Rust memory is not a correctness mechanism.

## PostgreSQL v1 record model

Every primary key, unique key, foreign key, lookup, and retention job includes
`tenant_id`. UUID alone is never treated as a tenant authorization boundary.

### `runs`

The row owns:

- tenant/run/thread identity and pinned graph/state/policy/provider versions;
- current validated lifecycle bytes and optimistic lifecycle revision;
- current journal sequence, event ID, recorded time, and digest;
- current checkpoint ID, superstep, and digest as one all-null or all-present
  pointer;
- last issued fencing epoch plus nullable active attempt/renewal/expiry;
- readiness time/priority/admission class and terminal/retention metadata;
- resolved budget, cumulative usage, and quarantine reason.

Journal sequence and fencing epoch use `BIGINT` with non-negative checks; active
tokens require positive values. `RunRevision` remains a full-width unsigned
wire value and uses `NUMERIC(20,0)` unless its accepted RFC narrows it before the
first database migration.

Head fields are all null only for an empty journal and all non-null otherwise.
Lease attempt/renewal/expiry are all null or all non-null. SQL check constraints
enforce these shapes in addition to application validation.

### `run_events`

Required columns include:

- tenant, run, positive sequence, and UUIDv7 event ID;
- database recorded time and source kind;
- nullable worker attempt and epoch with source-shape checks;
- event kind and pinned schema ID/version/digest;
- exact canonical payload bytes and byte length;
- payload, intent, nullable projection-intent, previous, and event digests.

The primary key is `(tenant_id, run_id, sequence)`. A unique constraint on
`(tenant_id, run_id, event_id)` implements idempotency. Sequence one requires a
null previous digest; later sequences require one. Payload size is checked
before insertion. Metadata may be projected into indexed columns, but neither
JSONB nor an ORM struct serialization becomes the integrity source.

### Checkpoints and attempts

`run_checkpoints` bind tenant/run, graph and state-schema versions, superstep,
parent checkpoint, the exact journal head, canonical inline state or an
integrity-bound blob reference, the sorted next-ready set, and checksum. Pending
node results are separate immutable rows anchored to the base checkpoint; a
successful barrier consumes them into the next checkpoint. A checkpoint is
usable only if its journal head is an ancestor of the current verified head.

The exact barrier, logical activation, stable reduction, and checkpoint
lineage semantics are defined by
[RFC-0002](0002-deterministic-graph-and-scheduler.md). The database must not
invent ordering from physical attempt or completion time.

`node_attempts` bind logical node/superstep identity, physical attempt,
deterministic input hash, status, pending/committed update, failure, usage, and
timestamps. Committed external model/tool results are reused after recovery;
they are not requested again merely to rebuild a process-local transcript.

### Invocation ledger, interrupts, timers, and outbox

- `tool_invocations` distinguishes prepared, executing, committed, failed, and
  unknown external outcomes; it retains the logical invocation and stable
  idempotency key across physical attempts.
- `interrupts` stores request payload, bound action digest, required principal
  and scopes, exclusive expiry, version, and one authenticated resolution.
- `timers` stores indexed due time and one firing event identity; the scheduler
  does not poll every suspended run.
- `outbox` stores an event/delivery ID, destination policy reference, payload
  digest/reference, next attempt, retry state, and terminal delivery evidence.
  Enqueue is atomic with the originating journal fact; delivery is at least
  once and receivers deduplicate by stable ID.

Large payloads use an S3-compatible object store through a prepare/commit
protocol. The database never commits a reference until size, media type,
tenant, checksum, encryption, and retention metadata validate. Orphaned uploads
are garbage-collected after a safety window.

## Recovery and corruption handling

Recovery performs:

1. load the tenant-scoped run row and pinned versions;
2. load the newest compatible checkpoint and verify its checksum/blob;
3. seed `JournalChainVerifier` from the checkpoint/archive head, or empty;
4. stream the ordered suffix and verify each event's payload, intent, event, and
   predecessor digests;
5. decode only registered supported schema versions;
6. rebuild or compare projections and cumulative usage;
7. require the computed final head to equal the run row;
8. make runnable work visible only after every check succeeds.

A checksum mismatch, sequence gap, unsupported required schema, missing blob,
cross-tenant reference, or projection disagreement quarantines the run. The
worker does not skip, repair, or reinterpret the record. Operator tooling can
export evidence, retry unavailable dependencies, restore a verified backup, or
run an explicit audited migration.

Snapshots reduce recovery work but never become a second independent source of
truth. Acknowledged journal facts remain the authority.

## Retention, archival, and legal hold

Retention is tenant-policy driven and terminal-state aware. Physical deletion
requires a durable tombstone and completion of dependent outbox, artifact, and
audit policies. Legal hold prevents destructive compaction and deletion.

Before deleting a journal prefix, StateKnot writes and verifies an immutable
archive plus a retained boundary `JournalHead`. The checkpoint/suffix verifier
starts after that head, preserving sequence and chain continuity. Archive
manifests include tenant/run ranges, schema versions, record count, first/last
heads, object checksum, encryption metadata, and retention policy. A database
administrator cannot merely delete old rows and advance the head.

## Migration and compatibility

Database migrations are ordered, checksum-pinned artifacts recorded in a
schema-migration table. Production rollout uses expand/migrate/contract:

1. expand with nullable/new tables and backward-compatible readers;
2. deploy dual-read or dual-write code with mismatch metrics where necessary;
3. backfill in bounded resumable tenant/key ranges with throttling;
4. validate counts, checksums, constraints, query plans, and restore behavior;
5. switch reads only after validation;
6. contract in a later release after the documented rollback window.

Migrations never silently reserialize integrity payloads. A content conversion
emits an explicit old-to-new conversion record, retains old digest/provenance,
and is idempotent. Interrupted backfills resume from durable progress. N-1 and
N-2 fixtures remain readable or have an offline migration tested before a
release claims support.

Schema DDL uses explicit types and constraints; application startup refuses a
database newer than its supported maximum or older than its required minimum.
Automatic destructive downgrade is not supported. Restore tests cover both the
current schema and supported upgrade paths.

## Security and privacy

- API authorization resolves tenant and principal before existence-disclosing
  queries; repository methods require tenant as a typed argument.
- Worker credentials can call only worker procedures/queries and cannot select
  a control-plane source or mutate lease epochs directly.
- Migration and repair roles are separate, audited, and unavailable to normal
  API/worker processes.
- Raw credentials never enter journal payloads, checkpoints, outbox records,
  errors, traces, or ordinary backups; opaque credential handles are resolved
  at call time.
- Payload and query byte limits are enforced before parsing and again by SQL
  checks where practical.
- Error messages and `Debug` output contain identifiers, sizes, kinds, and
  digests but not event data.
- Composite tenant foreign keys prevent accidental cross-tenant references.
  Row-level security may be added as defense in depth, but connection-pool
  session state is not the primary isolation mechanism.
- Encryption at rest, TLS, backup encryption, key rotation, access logging, and
  deletion/cryptographic-erasure procedures are deployment requirements.

## Observability and operations

Metrics use bounded labels and never tenant IDs, run IDs, event kinds from
untrusted extensions, or payload values as unbounded dimensions. Required
signals include:

- append/lock/commit latency and errors by trusted operation category;
- idempotent replay and conflicting event-ID counts;
- stale head, stale fence, expired lease, renewal, takeover, and epoch-exhaustion
  counts;
- runnable queue age, claim latency, lease age, scheduler fairness, and
  starvation;
- journal/checkpoint bytes, events per run, recovery suffix length, quarantine,
  and integrity failures;
- outbox age/retries/dead letters and artifact prepare/orphan counts;
- migration phase/progress/mismatch and backup/restore verification status;
- connection-pool saturation, lock waits, deadlocks, replica lag, WAL/storage
  growth, autovacuum health, and transaction age.

Alerts cover any integrity failure, accepted stale-write invariant breach,
growing quarantine/dead-letter populations, leases that cannot be recovered,
oldest runnable age, unsafe replica lag for the configured durability mode,
failed backup, and missed restore drill.

Operator runbooks must cover stuck leases, hot rows, event-ID conflict,
ambiguous external effects, corrupt history, missing artifacts, failed
migration, primary failover, point-in-time restore, legal hold, and tenant
deletion.

## Availability, backup, and disaster recovery

The release qualification configuration uses synchronous PostgreSQL replication
for acknowledged journal RPO 0. Client acknowledgement occurs only after the
primary reports commit under that configuration. During primary failover,
transactions may return retryable unavailable/indeterminate transport errors;
clients reuse the same event ID.

Backups combine PostgreSQL base backups/WAL needed for point-in-time recovery
with versioned object-store data and key metadata. A restore is performed into
an isolated environment, migrations run only under the documented path, and
all journal heads, checkpoint/blob digests, tenant references, runnable states,
and object availability validate before traffic. Backup existence alone is not
recovery evidence.

The qualification objectives remain those in the long-running scenario:
acknowledged database facts RPO 0, runnable recovery within 60 seconds after
database service restoration at reference load, and API/scheduler RTO within
five minutes after full application restart.

## Compatibility and dependencies

- PostgreSQL 16 and 17 are the first release qualification targets; later
  support is claimed only after migration, query-plan, failover, and restore
  evidence.
- All journal sequence and fencing epoch values fit signed `BIGINT`; their JSON
  forms remain decimal strings.
- Timestamps use canonical UTC microseconds, matching PostgreSQL `timestamptz`
  precision in the supported range.
- `serde_json_canonicalizer` is a mandatory MIT-licensed core dependency pinned
  in `Cargo.lock`; StateKnot adds duplicate-key/resource checks and I-JSON
  integer validation before invoking it.
- Canonical bytes and all three digest layers are cross-version fixtures.
  Changing a domain, field set, number rule, or serialization algorithm requires
  a new schema/digest version and explicit migration.

## Alternatives considered

### Update a snapshot without a journal

Rejected because it cannot prove which nondeterministic results and external
decisions committed, cannot support resumable event streams, and weakens audit
and recovery.

### Use an event table without an exact run head

Rejected because concurrent writers can both derive a next state and insert
incompatible facts. A unique sequence constraint detects a collision but does
not define deterministic idempotent resolution or atomic projections.

### Use lease expiry without fencing epochs

Rejected because a paused process can resume after another worker takes over.
Its local view of time cannot prove ownership. Every new claim must create a
higher database-observed token checked on every write.

### Use advisory locks as leases

Rejected because they are tied to sessions/transactions, do not provide a
durable portable fencing token, and are unsafe across pooled connection loss and
long external work.

### Hash compact `serde_json` or PostgreSQL JSONB

Rejected because ordinary compact JSON is not the cross-language RFC 8785
contract, while JSONB does not preserve exact input bytes. Integrity operations
must consume explicitly canonical bytes.

### Claim exactly-once external effects

Rejected because a database transaction cannot atomically commit with an
arbitrary provider or tool. StateKnot offers exactly-once database facts,
at-least-once delivery, stable idempotency where the peer supports it, and an
explicit unknown outcome otherwise.

## Validation and rollout

### Current implementation evidence

The unpublished `stateknot-store-postgres` crate implements the first
run/journal/checkpoint/lease subset of this RFC rather than a separate
transitional backend. Two exact migrations create tenant-scoped `runs`,
`run_events`, and immutable `run_checkpoints` records with database constraints;
runtime connection refuses absent, extra, failed, checksum-mismatched, or
incomplete migration state. Migration uses a separate temporary pool so DDL
credentials are not retained by the runtime.

The append implementation locks the run row, performs event-ID and exact
projection-intent idempotency before head/fence rejection, applies a supplied
`RunTransition` to the locked lifecycle, rejects observations later than the
commit clock, and atomically commits canonical event bytes, the complete head,
and the lifecycle projection. Checkpoint append additionally commits the exact
parented graph/state barrier and current pointer in that transaction. Worker
event, checkpoint, and head writes repeat the exact attempt/epoch,
exclusive-expiry, and checkpoint-parent predicates in SQL. Reads reconstruct
every integrity layer, verify a checkpoint's exact journal-anchor event, and
stream-verify the suffix to the exact run head.

Thirteen integration tests run against digest-pinned PostgreSQL 16 and 17. They
cover fresh migration, startup refusal, existing v1-history upgrade, admission,
event/projection/checkpoint conflicts and lost acknowledgements,
renewal/expiry/release/supersession, stale fences including retry after takeover,
failures injected after event and checkpoint insertion, bounded suffix paging,
corrupted checkpoint/anchor bytes, invalid/future lifecycle transitions, 100
concurrent journal appenders, and 24 competing checkpoint writers producing one
contiguous lineage. This is evidence for those boundaries only. Pending node
writes, attempt/invocation ledgers,
automatic quarantine, role-separated database procedures, the 10,000 stale-race
trial, failover, archive, backup/restore, and soak gates below remain incomplete;
the RFC therefore remains Draft.

Before RFC acceptance:

- all core types have closed schemas, strict Serde, versioned fixtures, RFC 8785
  vectors, boundary tests, redacted diagnostics, and randomized state tests;
- a real PostgreSQL 16/17 implementation passes append idempotency, head race,
  renewal, expiry, revocation, and 10,000 forced stale-worker race trials with
  zero accepted stale writes;
- kill points before/after every insert, projection, head update, commit, and
  acknowledgement recover to one valid outcome;
- 100 concurrent appenders preserve one contiguous history without lost
  projections or duplicate event identity;
- primary failover and lost acknowledgements converge through same-ID retry;
- corrupted payload, intent, event, predecessor, checkpoint, and blob digests
  quarantine without unsafe execution;
- N-1/N-2 forward migration, interrupted backfill, rollback-window, and
  newer-schema refusal tests pass;
- archive/compaction retains a verified boundary and legal hold blocks deletion;
- backup plus point-in-time restore passes full integrity and runnable-state
  validation on the reference dataset;
- scenario latency, memory, recovery, and 24-hour soak thresholds pass.

Rollout proceeds from deterministic single-process integration tests, to local
PostgreSQL fault tests, to a multi-replica staging cluster, then controlled
production pilots. No in-memory backend may be used to substantiate a
production durability claim.

## Unresolved questions

No unresolved question changes the implemented canonical journal, append
identity, or fencing contracts. Exact DDL indexes, partition thresholds,
checkpoint frequency, archive cadence, lease duration, and timeout defaults
must be selected from committed benchmarks before this RFC can move from Draft
to Accepted.
