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
  complete checkpoint integrity validation;
- `CheckpointLineageVerifier`: constant-memory newest-to-oldest validation from
  an exact compact tip through the superstep-zero root.
- `ToolInvocationIntent`, `ToolInvocation`, and `ToolInvocationHead`: immutable
  execution intent, closed lifecycle revision, exact journal/predecessor
  anchoring, and a complete optimistic comparison token;
- `ToolInvocationHistoryVerifier`: constant-state ascending validation of
  transition legality, hash links, safe retries, and exact paged continuation.
- `ModelInvocationIntent`, `ModelInvocation`, and `ModelInvocationHead`:
  negotiated immutable descriptor/request intent, one physical attempt per
  provider exchange, complete response or public-safe failure evidence, exact
  journal/predecessor anchoring, and closed prepared/executing/committed/failed
  revisions;
- `ModelInvocationHistoryVerifier`: constant-state ascending validation of
  transition legality, attempt provenance, explicit delayed retry evidence,
  hash links, and complete recovery history.
- `NodeAttemptStart`, `NodeAttemptCompletion`, and `NodeAttempt`: a durable
  pre-execution claim with node/worker attempt separation, exact activation and
  journal integrity, append-only success/failure completion, usage evidence,
  explicit delayed retry, higher-epoch takeover of unfinished work, and a
  streaming recovery-history verifier.

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

async fn load_checkpoint_lineage_page(
    &self,
    tenant_id: &TenantId,
    run_id: RunId,
    from: Option<&CheckpointHead>,
    page_size: CheckpointLineagePageSize,
) -> Result<CheckpointLineagePage, StoreError>;

async fn prepare_tool_invocation(
    &self,
    append: JournalAppend,
    intent: ToolInvocationIntent,
) -> Result<ToolInvocationCommitOutcome, StoreError>;

async fn advance_tool_invocation(
    &self,
    append: JournalAppend,
    expected: &ToolInvocationHead,
    transition: ToolInvocationTransition,
) -> Result<ToolInvocationCommitOutcome, StoreError>;

async fn load_tool_invocation(
    &self,
    tenant_id: &TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
) -> Result<ToolInvocation, StoreError>;

async fn load_tool_invocation_history_page(
    &self,
    tenant_id: &TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    after: Option<&ToolInvocation>,
    page_size: ToolInvocationHistoryPageSize,
) -> Result<ToolInvocationHistoryPage, StoreError>;

async fn prepare_model_invocation(
    &self,
    append: JournalAppend,
    intent: ModelInvocationIntent,
) -> Result<ModelInvocationCommitOutcome, StoreError>;

async fn advance_model_invocation(
    &self,
    append: JournalAppend,
    expected: &ModelInvocationHead,
    transition: ModelInvocationTransition,
) -> Result<ModelInvocationCommitOutcome, StoreError>;

async fn load_model_invocation(
    &self,
    tenant_id: &TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
) -> Result<ModelInvocation, StoreError>;

async fn load_model_invocation_history_page(
    &self,
    tenant_id: &TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    after: Option<&ModelInvocation>,
    page_size: ModelInvocationHistoryPageSize,
) -> Result<ModelInvocationHistoryPage, StoreError>;

async fn load_runnable_run_page(
    &self,
    tenant_id: &TenantId,
    cursor: Option<&RunnableRunPageCursor>,
    page_size: RunnableRunPageSize,
) -> Result<RunnableRunPage, StoreError>;
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
    head/fence rejection, reject any non-committed invocation rooted at that
    exact parent, then insert the immutable checkpoint anchored to the event
    created in step 9;
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

The scheduler discovers candidates with a tenant-scoped indexed readiness
query. Discovery is read-only: it does not use `FOR UPDATE SKIP LOCKED`, does
not reserve a row, and is never interpreted as proof that global work is
absent. The first bounded page fixes a PostgreSQL transaction timestamp; every
opaque continuation keeps that cutoff and advances by
`(effective_available_at, run_id)`. Effective availability is the later of the
database-observed queue-entry time and any durable lease expiry. New admission,
release, or runnable lifecycle transition receives a later database observation
and therefore cannot be inserted behind an existing cursor. Waiting, terminal,
and quarantined rows are excluded.

After policy selects one exact candidate, `claim_lease` takes that run lock and:

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
- nullable database-observed scheduler readiness, with waiting and terminal
  states required to be null; future priority/admission-class and
  terminal/retention metadata;
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

The core `PendingNodeResultIntent` is the logical idempotency value. It binds
the base checkpoint, graph namespace, node ID, activation input digest, bounded
schema-pinned update, closed route/wait/terminal/continue outcome, and a
canonical set of exact committed model/tool revision heads. The immutable
`PendingNodeResult` additionally binds the winning worker attempt/epoch and its
exact journal head. SQL must prove the base checkpoint and every invocation
revision through composite tenant/run foreign keys, verify the worker source
matches the stored fence, and reject a result anchor that does not follow all
dependencies. Consumption metadata may advance once to one exact successor
checkpoint, but no integrity-bearing result field is updated.

Migration 5 and the current store implement the immutable write/read and atomic
consumption boundary of this contract. `pending_node_results` has one logical activation key, exact
base-checkpoint and worker-event/fence foreign keys, bounded canonical bytes,
and separate tool/model binding tables whose ordinary composite foreign keys
prove exact activation plus committed revision. Identical semantic retries
return the original winner even after lease takeover; changed semantics
conflict. Recovery revalidates the full record, binding projections, full
invocation records, and journal anchors in one repeatable-read snapshot. The
barrier APIs revalidate those full records outside the run lock, compare the
complete compact set under the lock, and append exact consumption rows with the
successor event/checkpoint and run heads in one transaction.

The exact barrier, logical activation, stable reduction, and checkpoint
lineage semantics are defined by
[RFC-0002](0002-deterministic-graph-and-scheduler.md). The database must not
invent ordering from physical attempt or completion time.

`node_attempts` bind logical node/superstep identity, a run-wide unique physical
node attempt, its distinct authorizing worker fence, deterministic input hash,
start event, and start time before user code executes. An append-only completion
binds exactly one success or failure plus usage and its later event/time.
Success references the pending result committed in that same transaction;
failure retains public-safe evidence and explicit recovery advice. A missing
completion is recoverable only after the worker fence is superseded. Committed
external model/tool results are reused after recovery; they are not requested
again merely to rebuild a process-local transcript. Migration 6 implements the
tables and atomic start/fail/succeed transactions, bounded fully verified
history, database-clock retry gates, and truthful migration-5 result recovery.

Migration 7 adds `scheduler_ready_at`, backfills runnable migration-6 rows
without fabricating lifecycle events, enforces runnable/non-runnable shape with
a validated check constraint, and creates a partial expression index on
`(tenant_id, greatest(scheduler_ready_at,
coalesce(lease_expires_at, scheduler_ready_at)), run_id)` for non-quarantined
runnable rows. Admission stores its database observation;
runnable lifecycle transitions and orderly lease release requeue at their
commit observation; waiting or terminal transitions clear the projection.
Claim, renewal, and supersession retain queue entry while lease expiry delays
effective availability, so no per-run timer update is needed at expiry.

### Invocation ledger, interrupts, timers, and outbox

- `tool_invocations` stores one immutable canonical intent with the complete
  descriptor/input/effective-limit snapshot, exact base-checkpoint activation,
  and an exact current-revision/status/attempt/digest pointer.
- `tool_invocation_revisions` stores canonical full records with contiguous
  revision, exact predecessor and journal heads, transition/status projections,
  and a run-wide unique physical attempt claim. The closed states are prepared,
  executing, committed, failed, and unknown; the logical invocation and stable
  peer idempotency key survive physical attempts. Unknown outcomes admit only a
  reconciliation transition, and retryable failures retain the evidence and
  durable not-before time needed to prove a later attempt safe.
- A successor checkpoint cannot commit while an invocation rooted at its exact
  parent is prepared, executing, failed, or unknown. A committed invocation
  releases this orphan-prevention guard; the pending node-result ledger must
  separately prove consumption into the barrier state.
- `model_invocations` stores one negotiated immutable model descriptor/request
  snapshot and exact base-checkpoint activation. `model_invocation_revisions`
  stores hash-linked prepared, executing, failed, and committed records. Every
  real provider exchange has a fresh run-wide unique physical `AttemptId`; the
  provider call starts only after its executing revision is durable.
- A model response commits only after exact attempt/model/request validation.
  A stream is merely transient evidence until it closes into one complete
  `ModelResponse`; partial content never becomes a committed result. A failed
  invocation may start another attempt only when its explicit `RetryAdvice` is
  `safe_after`, the journal clock proves the full delay elapsed, and the new
  attempt identity differs. SDK-level hidden retries violate this contract.
- `run_attempt_claims` is the one run-wide physical-attempt namespace shared by
  node, tool, model, and outbox ledgers. Its primary key rejects reuse; generated kind
  plus deferred composite foreign keys bind tool/model claims to exact
  invocation revisions and node claims to exact immutable starts. Migration 4
  backfills all v3 tool attempt claims before model dispatch becomes possible;
  migration 6 extends the same namespace to node execution, and migration 8
  adds exact delivery/epoch ownership while retaining non-outbox anchor
  uniqueness through a partial index.
- `interrupts` stores request payload, bound action digest, required principal
  and scopes, exclusive expiry, version, and one authenticated resolution.
- `timers` stores indexed due time and one firing event identity; the scheduler
  does not poll every suspended run.
- `outbox` stores a stable `DeliveryId`, the exact originating `JournalHead`, an
  immutable tenant-owned destination snapshot reference, a schema-pinned
  canonical payload, an exclusive delivery deadline, retry state, and bounded
  terminal evidence. `OutboxDeliveryIntent` names the origin `EventId` before
  commit; the store materializes it only against that exact journal head and
  inserts both in one transaction. Retrying an enqueue succeeds only when the
  complete intent digest matches.
- Every network request first commits a fresh `AttemptId` and exact successor
  delivery fencing epoch under a fixed, non-renewable lease. The full request
  timeout must be below that lease, the lease is at most five minutes, and a
  completion is accepted only before its exclusive database-clock expiry.
  Unfinished attempts may be replaced only after expiry. Safe failures retry
  only after their durable `safe_after` boundary; `never`, 64 attempts, or the
  delivery deadline terminates in dead-letter/expiry. Acknowledgement is
  absorbing.
- Delivery is at least once. A lost acknowledgement can therefore cause the
  same `DeliveryId` and payload to be sent again. Only duplicate-tolerant
  notification protocols may use this outbox; ambiguous non-idempotent model
  and tool effects remain in their invocation ledgers. A protocol binding may
  carry `DeliveryId` in a negotiated header, claim, or extension, but the core
  does not claim that A2A or every webhook receiver standardizes such a field.
  Raw credentials never enter the destination snapshot, payload, failure, or
  acknowledgement evidence; adapters resolve scoped credential handles only
  at dispatch.

Large payloads use an S3-compatible object store through a prepare/commit
protocol. The database never commits a reference until size, media type,
tenant, checksum, encryption, and retention metadata validate. Orphaned uploads
are garbage-collected after a safety window.

## Recovery and corruption handling

Runnable discovery is only an index-backed candidate source. Cross-tenant
weighting, fairness, admission classes, and dispatch limits belong to the
scheduler policy in RFC-0002. A scheduler must claim one selected run with a
fresh physical attempt identity and treat `LeaseHeld` as normal contention
before recovery performs:

1. load the tenant-scoped run row and pinned versions;
2. load the newest compatible checkpoint and verify its checksum/blob;
3. seed `JournalChainVerifier` from the checkpoint/archive head, or empty;
4. stream the ordered suffix and verify each event's payload, intent, event, and
   predecessor digests;
5. decode only registered supported schema versions;
6. rebuild or compare projections and cumulative usage;
7. require the computed final head to equal the run row;
8. make runnable work visible only after every check succeeds.

The implementation starts this path with `begin_claimed_run_recovery`, which
requires one exact live `RunFence` and the candidate's exact journal
observation. Its bounded lineage, journal, invocation-history, node-attempt, and
pending-result reads share one stable corruption evidence identity; recovery
revalidates the same fence and head before handing work to a durable start
transaction. Its ready-node planner pins the current checkpoint, streams
verified pending results and complete histories in bounded pages, classifies
the exact root ready set at database time, and binds the canonical plan to the
final journal observation. `start_recovered_node_attempt` accepts only a
dispatchable plan decision and then repeats the current checkpoint,
latest-predecessor transition, retry-time, lifecycle, journal, and live-fence
checks while atomically committing the start before node code. Only a new
`Committed` outcome grants that caller launch authority; an `Idempotent` start
remains in flight until its owner finishes or a higher fence takes it over. A
recovery-originated quarantine persists the detecting attempt and epoch in its
versioned digest and repeats the exact unexpired-fence predicate while revoking
the lease. Consequently, a worker
superseded without an intervening journal event cannot use subsequently
observed corruption to stop the successor. Unfenced control-plane quarantine
remains explicit and retains its v1 audit format.

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
run/journal/checkpoint/invocation/node-attempt/outbox/durable-wait/quarantine/lease
subset of this RFC rather than a separate transitional backend. Ten exact migrations
create tenant-scoped `runs`, `run_events`, immutable `run_checkpoints`,
tool/model intent and revision ledgers, the shared `run_attempt_claims`
registry, pending results and consumptions, transactional outbox records, and
durable interrupt/timer evidence and out-of-journal quarantine observations
with database constraints; runtime connection
refuses absent, extra, failed,
checksum-mismatched, or incomplete migration state. Migration uses a separate
temporary pool so DDL credentials are not retained by the runtime. Migration 4
backfills every v3 tool attempt and installs exact claim foreign keys before it
admits model history.

The append implementation locks the run row, performs event-ID and exact
projection-intent idempotency before head/fence rejection, applies a supplied
`RunTransition` to the locked lifecycle, rejects observations later than the
commit clock, and atomically commits canonical event bytes, the complete head,
and the lifecycle projection. Checkpoint append additionally commits the exact
parented graph/state barrier and current pointer in that transaction. Worker
event, checkpoint, and head writes repeat the exact attempt/epoch,
exclusive-expiry, and checkpoint-parent predicates in SQL. Reads reconstruct
every integrity layer, verify a checkpoint's exact journal-anchor event, and
stream-verify the suffix to the exact run head. Reverse checkpoint-lineage reads
use hard-bounded repeatable-read pages, exact full-head cursors, recursive parent
identity joins, and batched fully decoded journal-anchor verification. Immutable
continuations remain valid when a later barrier advances the run pointer.

Tool preparation and transition lock the run plus invocation, apply the closed
core state machine, and atomically commit the event, immutable revision, exact
current pointer, and run journal head. Every worker mutation repeats the live
attempt/epoch/exclusive-expiry predicate in SQL. Same-event retries compare the
complete canonical intent/transition result before converging. Reads reconstruct
canonical bytes and redundant projections, validate the base checkpoint and
exact journal projection binding, and stream-verify hard-bounded ascending
history pages from complete cursors. Until the checkpoint contract represents
namespaced ready activations, preparation accepts only a root namespace and a
node present in the exact base checkpoint's `ReadyNodes`; all other activations
fail closed before an event is inserted. Preparation and `StartAttempt` require
an active run; cancellation and waiting may accept outcome/reconciliation
evidence for work already in flight but cannot dispatch new tool work. Under the
same locked run row, checkpoint advancement rejects any non-committed invocation
rooted at the exact current checkpoint. This prevents an in-flight external call
from being orphaned; the barrier transaction separately proves and records the
exact complete pending-result consumption set.

Model preparation and transition use the same atomic and fencing boundary with
the model-specific closed state machine. The intent snapshots the negotiated
descriptor and complete request once; compact revisions retain exact intent,
predecessor, journal, transition, response/error, and attempt bindings without
repeating the request bytes. Reads rejoin and canonicalize both layers. A new
provider exchange requires a fresh run-wide attempt claim, explicit failed-state
`SafeAfter` evidence, and elapsed database-recorded journal time. Complete
responses/errors may finish after waiting or cancellation intent, but new
preparation and dispatch require an active run. Model and tool claims cannot
cross even when invocation identifiers collide.

Durable-wait commits persist the canonical interrupt/timer batch with the
waiting lifecycle and either an initial checkpoint or a complete successor
result barrier. Database-clock terminal APIs enforce authorization, exclusive
interrupt expiry, and inclusive timer due time while atomically advancing the
lifecycle and run head. Fixed-cutoff, tenant-scoped partial-index pages discover
due timers and expired interrupts without per-run polling. Cancellation or
failure from waiting records an append-only abandonment fact for every
outstanding wait; migration 9 quarantines evidence-free legacy waiting rows
rather than fabricating requests, deadlines, authority, or journal anchors.

Migration 10 and `quarantine_run` persist one immutable observation outside a
journal that may itself be corrupt. The request binds a stable quarantine ID,
closed cause, bounded non-secret component code, caller-retained evidence
digest, and exact journal observation. The transaction uses database time,
clears active execution ownership, and sets the run quarantine projection that
excludes scheduler discovery. Exact retries survive a lost acknowledgement;
different evidence conflicts. Pre-v10 quarantines remain blocked but do not gain
invented observation records.
`with_corruption_quarantine` composes this transaction with one read-only
recovery validation: success and non-corruption failures pass through, while a
payload-redacted integrity failure deterministically supplies the component
code. The recovery read finishes before the separate quarantine transaction;
an advanced journal head returns a stale-observation error rather than stopping
newer state.

Ninety-eight provider integration tests run against PostgreSQL 16 and 17.
They cover fresh migration, startup refusal, existing v1-history upgrade, v3
tool-attempt backfill into the exact shared registry, admission,
event/projection/checkpoint conflicts and lost acknowledgements,
renewal/expiry/release/supersession, stale fences including retry after takeover,
failures injected after event, checkpoint, invocation-revision, and attempt-claim
insertion, bounded suffix and reverse-lineage paging, exact/crossed cursor
rejection, continuation after a later checkpoint commit, a missing page-edge
parent, corrupted checkpoint/invocation/anchor bytes and projection bindings,
invalid/future lifecycle transitions, model delayed retry and exact response
provenance, cross-tool/model attempt rejection, 100 concurrent journal
appenders, 24 competing checkpoint writers producing one contiguous lineage,
and 24 competing tool/model invocation writers admitting exactly one physical
attempt. Pending-result tests additionally cover exact committed tool/model
bindings, same-event and cross-fence semantic retries, cancellation precedence,
stale fencing, full recovery after corruption attempts, rollback of
event/result/bindings/head on an invalid reference, and 24 contenders producing
one physical winner. Non-ready and unsupported nested activations are rejected without
durable residue, and cancellation races retain in-flight results without
admitting new work. Checkpoint advancement is rejected until exact-parent
invocations commit. Atomic barrier tests additionally cover complete-set
consumption, lost acknowledgements, stale fences, injected rollback, and
24-writer linearity. Node-attempt coverage proves durable start and terminal
commit idempotency, success/result/barrier binding, same-epoch retry rejection,
higher-fence recovery, database-time delayed retry, bounded history, run-wide
cross-kind attempt identity, and fail-closed corrupted-byte recovery. The core
also freezes integrity-bound interrupt request/resolution and timer
registration/firing records with exact journal causality, principal/scope
authorization, exclusive expiry, inclusive due time, tamper rejection, and
versioned canonical fixtures. Their PostgreSQL coverage adds exact v8 upgrade,
indexed discovery, initial/successor checkpoint atomicity, lost-ACK convergence,
fencing, abandonment audit, corruption and rollback rejection, plus 24-request
single-commit convergence. Quarantine coverage adds v9 upgrade honesty, exact
journal observation fencing, atomic lease removal and scheduler exclusion,
same-ID lost-ACK recovery, corruption/rollback rejection, and 24-request
single-record convergence. A real corrupted-checkpoint recovery read also
proves corruption-only automatic isolation, ordinary-error pass-through,
idempotent retry, and stale-observation rejection. Ready-node recovery coverage
adds deterministic fresh dispatch, higher-fence crash takeover, immutable
result reuse as barrier input, noncanonical-activation admission rejection,
ordinary pre-checkpoint handling, plan scoping, lost-ACK convergence, and 24 identical
starts producing one physical commit on PostgreSQL 16/17. The 64-attempt hard
ceiling is also recovery-readable, rejects a 65th new start without journal
residue, and preserves exact lost-ACK idempotency at the boundary. This is
evidence for those boundaries only. Migration 12 additionally proves
plan-bound delayed wakeup, direct-claim gating, no-polling indexed visibility,
lost-ACK convergence, due-race lease retention, and exact v11 upgrade on
PostgreSQL 16/17. Migration 13 proves immutable tenant-scoped compiled-graph
registration, identical-byte idempotency, version conflict, exact v12 upgrade,
canonical-byte/projection corruption rejection, tenant isolation, checkpoint-pin
revalidation with fenced quarantine, and a 24-way conflicting registration race
on both database versions. The unpublished runtime now adds an offline exact
schema/reducer/node executable closure, bounded noninitial replay, and a fenced
root Graph Driver with durable-before-dispatch starts, pre-launch lease refresh,
monotonic expiry enforcement, lease renewal, Continue barrier commits, and
typed lifecycle handoffs. Root-to-terminal recovery, same-fence suppression,
near-expiry launch protection, long-running renewal, and higher-fence takeover
pass on both database versions. The lifecycle coordinator now atomically
consumes Wait/Terminal/failure handoffs, and the tenant-scoped Agent Loop closes
one bounded discovery-to-lifecycle quantum with exact lost-ack recovery.
Provider-neutral model/tool attempts now execute through exact immutable
provider registries, durable starts, streaming validation, ambiguity-safe tool
failures, and no-dispatch terminal recovery. Migration 14 and the runtime fair
scheduler add immutable weighted policies, globally ordered lost-ACK-safe
reservations, explicit starvation bounds, and bounded retention. Migration 15
and the runtime admission facade atomically bind authenticated intent,
database-clock admission, Active lifecycle, sequence-one event, superstep-zero
checkpoint, and scheduler visibility, with exact retries and complete integrity
reloads. Role-separated database procedures, the complete durable public
run/result integration and transcript assembly inside the prebuilt public Agent
graph, ingress idempotency-key mapping, the 10,000 stale-race trial, failover,
general archive, backup/restore, and soak gates below remain incomplete; the RFC
therefore remains Draft.

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
