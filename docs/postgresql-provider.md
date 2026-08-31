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
- migration-13 immutable compiled-graph definitions scoped by tenant plus exact
  owner/name/version identity. Registration accepts one canonical definition,
  converges only for identical bytes, and rejects version reuse; every load
  recompiles the closed descriptor and checks its digest and redundant lookup
  columns before returning it;
- atomic control-plane and worker initial-checkpoint APIs whose event,
  lifecycle projection, checkpoint row, journal head, and checkpoint pointer
  commit or roll back together; raw successor writes fail with
  `CheckpointBarrierRequired`;
- immutable tool- and model-invocation intents and hash-linked revisions with
  exact base-checkpoint and journal anchors, stable logical versus physical
  attempt identities, bounded ascending history verification, closed tool
  prepared/executing/committed/failed/unknown outcomes, and closed model
  prepared/executing/committed/failed outcomes;
- a run-wide node/tool/model/outbox physical-attempt registry whose primary key
  prevents cross-ledger reuse and whose deferred exact-owner foreign keys make
  each claim inseparable from its immutable node start or invocation revision;
- atomic fenced prepare/advance APIs whose event, invocation revision, current
  invocation pointer, and run journal head commit or roll back together, with
  exact lost-ack convergence and no blind retry from an unknown outcome;
- durable physical node-attempt starts committed before user code dispatch,
  append-only success/failure completions, database-clock delayed-retry gates,
  higher-fence takeover of abandoned execution, bounded verified history, and
  atomic successful completion with its attempt-owned pending result;
- immutable pending node results committed atomically with their worker event,
  exact winning lease fence, run journal head, canonical semantic intent, and
  separately foreign-keyed committed tool/model revisions; logical retries
  converge on the original physical winner across lease takeover;
- repeatable-read pending-result recovery that revalidates canonical bytes,
  every redundant SQL projection, the base checkpoint, worker event source and
  projection digest, binding rows, full invocation intents/revisions, and all
  invocation journal anchors in bounded batches;
- two-record unconsumed pending-result pages with compact look-ahead rows,
  canonical `(graph_namespace, node_id)` order, full-record verification, and a
  cursor that pins the base checkpoint, last result head, and run journal head;
  concurrent result commits make continuation explicitly stale;
- atomic control-plane and fenced-worker barrier APIs that verify the base and
  every full result in a repeatable-read preflight, lock the run, recheck the
  exact complete compact result set, and commit the projection-bound event,
  successor checkpoint, append-only consumption rows, lifecycle, journal head,
  and checkpoint pointer in one transaction;
- migration-9 durable interrupt/timer evidence with canonical immutable
  registrations, resolutions and firings, mutable terminal projections backed
  by deferred exact foreign keys, database-clock exclusive expiry/inclusive due
  enforcement, and tenant-scoped bounded due/expiry keyset pages. Initial and
  successor wait-barrier APIs atomically join event, checkpoint, result
  consumption, full wait batch, lifecycle, journal, and checkpoint pointers;
  cancellation/failure records one immutable abandonment fact per outstanding
  wait instead of silently discarding evidence;
- successor-checkpoint rejection while any invocation rooted at the exact
  current checkpoint remains prepared, executing, failed, or unknown;
- database-clock lease claim, renewal, release, forced supersession, monotonically
  increasing fencing epochs, and exact worker predicates on both event and run
  head writes;
- migration-7 runnable readiness projected into every admission, lifecycle
  transition, and lease release; a partial tenant/exact-availability/run index;
  and stable keyset pages fixed to a database transaction timestamp, bounded to
  16 fully decoded runs plus one look-ahead row. Live leases delay availability
  until their exclusive expiry without a timer update, while waiting, terminal,
  and quarantined runs stay outside the hot index;
- migration-12 delayed retry wakeups that keep queue age separate from an
  inclusive `scheduler_not_before` gate. A deferred-only ready-node plan
  revalidates its exact checkpoint, journal, live fence, lifecycle, and database
  clock before releasing ownership atomically; ordinary claims cannot bypass
  the gate, due runs reappear through the same partial index without a polling
  update, and lost acknowledgements converge on the stored boundary;
- migration-8 transactional outbox storage with immutable tenant-owned
  destination snapshots, schema-pinned canonical payloads, and non-empty
  event-scoped batches of at most 64 deliveries. Control-plane and worker
  enqueue APIs commit the exact journal event, complete delivery set, run head,
  and lifecycle projection together; retries compare the whole set, and worker
  inserts repeat the live run-fence predicate in SQL;
- tenant-scoped `SKIP LOCKED` delivery claims backed by a partial ready index.
  Each claim inserts a fresh run-wide `AttemptId`, monotonic delivery epoch, and
  canonical fixed start before returning anything dispatchable. Leases are
  non-renewable and bounded to five minutes; ACK/failure completion requires
  the exact live fence and database clock, while identical lost-ACK retries
  converge and conflicting evidence fails closed;
- explicit at-least-once recovery: unfinished requests may be replaced only
  after their fixed expiry, `SafeAfter` uses a durable database-time boundary,
  `Never` and attempt 64 dead-letter, and a bounded indexed reaper projects
  deadline expiry or an abandoned 64th attempt without fabricating completion
  evidence. All reads and claims revalidate delivery bytes, origin journal,
  destination snapshot, complete attempt/completion history, and redundant SQL
  projections;
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
`stateknot.model_invocation_revisions`, `stateknot.run_attempt_claims`,
`stateknot.node_attempts`, `stateknot.node_attempt_completions`,
`stateknot.pending_node_results`, both pending-result binding tables, and
`stateknot.pending_node_result_consumptions`; `SELECT`/`INSERT` on
`stateknot.outbox_destinations`, `stateknot.outbox_attempts`, and
`stateknot.outbox_attempt_completions`; and `SELECT`/`INSERT` plus only the
claim/completion/reaper projection columns on `stateknot.outbox_deliveries`.
It also needs `SELECT`/`INSERT` on `stateknot.interrupt_resolutions`,
`stateknot.timer_firings`, and `stateknot.wait_abandonments`, plus
`SELECT`/`INSERT` and terminal-projection-only `UPDATE` on
`stateknot.run_wait_registrations`; and `SELECT`/`INSERT` on
`stateknot.run_quarantines` and `stateknot.graph_definitions`.
Do not grant runtime DDL,
checkpoint, node-attempt, invocation-revision, pending-result, or consumption
update/delete permissions. Exact role/grant SQL will be
shipped only with the role-separated server boundary so this document does not
invent deployment-specific role names.

`PostgresTransportSecurity::VerifyFull` is the default and overrides weaker URL
settings. `RequireEncryption` deliberately forgoes server-identity verification.
`Disabled` is only for a trusted local socket or isolated test network.

## Validation

The current database suite runs 91 integration tests against PostgreSQL 16 and
17.
They cover fresh migration, startup refusal, an existing v1 history upgrading to
v8 without guessed projection or physical-attempt provenance, real v3
tool-attempt history backfilled into the run-wide registry, admission, direct
lifecycle transition enforcement, future-clock rejection, event and projection-intent
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
attempt. Pending-result coverage proves exact tool/model binding recovery,
same-event lost-ack retry, semantic retry after lease takeover, cancellation
precedence, stale-fence rejection, corrupted-byte rejection, rollback of the
event/result/bindings/head unit, and 24 contenders converging on one physical
winner. A nine-binding recovery case crosses the one-model, two-tool, and
eight-event verification batch boundaries so maximum record sizes cannot turn
the 256-reference logical ceiling into unbounded provider memory. Model
coverage additionally proves delayed retry, cross-tool/model
attempt rejection, exact response provenance, and rollback of an event,
revision, current pointer, and attempt claim as one unit. Invalid non-ready and
nested-namespace activations leave no event or invocation row; cancellation
blocks new tool/model work while preserving already executing outcome evidence.
A checkpoint cannot advance past a prepared or otherwise unsettled invocation,
and a committed result releases that guard. Barrier coverage proves complete
ready-set consumption, missing/conflicting result rejection, stale fencing,
lost-ack idempotency after lease takeover, rollback when consumption insertion
fails, 24 identical contenders producing one physical commit, and 24
concurrent writers producing a contiguous 49-event result/barrier lineage.
Node-attempt coverage proves durable start and terminal lost-ack convergence,
atomic success/result/barrier binding, same-epoch unsafe retry rejection,
higher-fence recovery of unfinished execution, database-time delayed retry,
bounded exact history paging, cross-kind run-wide attempt identity, corrupted
start/completion rejection, and migration-5 result readability without
fabricating a historical start. Scheduler coverage additionally proves exact
migration-6 readiness backfill, presence of the partial expression index and
validated shape constraint, startup refusal after constraint removal,
tenant-scoped cursor rejection, fixed-snapshot continuation across release and
new admission, database-observed lifecycle requeue, terminal removal, automatic
visibility after lease expiry, and one winner among 24 concurrent exact-run
claims.

Outbox coverage proves exact v7-to-v8 upgrade while preserving existing
node/tool/model claims, required partial indexes and the ready-query plan,
destination registration and tenant isolation, empty/duplicate/oversized batch
rejection, missing-destination and injected post-delivery rollback, exact whole-set
enqueue idempotency, delivery-ID conflict, worker-fence takeover with committed
lost-ACK recovery, 24 concurrent unique claims, durable-before-dispatch rows,
ACK and failure lost-ACK convergence, conflicting evidence rejection, fixed-lease
takeover, database-time `SafeAfter`, absorbing `Never`, deadline expiry, four-page
64-record history, completed and abandoned attempt-64 dead-letter paths, and
fail-closed destination/delivery/origin/start/completion corruption. The same
seven new scenarios run against PostgreSQL 16 and 17; the hard-limit case uses
real transactions and canonical history rather than injecting a projection.

Durable-wait coverage proves fresh and exact v8-to-v9 migration, quarantine of
legacy waiting lifecycles without fabricated evidence, atomic initial and
successor wait-barrier commits, generic-projection bypass rejection, principal
and scope authorization, exclusive interrupt expiry, inclusive timer due time,
fixed-cutoff tenant-scoped discovery, exact lost-ACK retries, cancellation and
failure abandonment, immutable audit loading, injected rollback of every joined
projection, fail-closed digest/reason corruption, released-fence retry, and 24
identical interrupt resolutions converging on one physical commit.

Run-quarantine coverage proves exact v9-to-v10 migration without fabricating
evidence for legacy quarantines, bounded machine-code validation, exact journal
observation fencing, atomic lease removal and runnable-index exclusion,
same-ID lost-ACK recovery, cross-tenant isolation, injected projection rollback,
fail-closed record-digest corruption, and 24 identical requests converging on
one immutable observation. The recovery-read combinator additionally proves
that successful reads pass through, ordinary not-found errors do not quarantine,
real corrupted checkpoint bytes do quarantine with a derived non-secret
component, exact retries converge, and stale observations cannot stop a newer
head. Migration 11 and claimed-recovery coverage preserve existing unfenced v1
evidence byte-for-byte while adding optional attempt/epoch evidence under a v2
record digest. A `ClaimedRunRecovery` starts only for an exact live fence and
journal observation, passes ordinary read errors without isolation, pages the
checkpoint/journal and invocation/node-result histories through the same stable
quarantine intent, and revalidates ownership before handoff. The race test
supersedes that fence before exposing real corrupted checkpoint bytes: the old
session returns `StaleFence` and leaves the successor running; only the current
owner may commit quarantine. Ready-node recovery additionally proves canonical
fresh dispatch, higher-fence unfinished-attempt takeover, attempt-owned result
reuse as exact barrier input, ordinary missing-checkpoint handling, activation
input-drift admission rejection with zero residue, plan scope rejection,
lost-ACK convergence, and 24 identical starts producing one new `Committed`
launch grant plus 23
non-dispatching `Idempotent` observations. A 64-attempt history remains fully
recoverable as `Exhausted`, the 65th unique start leaves no residue, and exact
lost-ACK replay at the ceiling remains idempotent. Graph-registry coverage proves
fresh and exact v12-to-v13 migration, schema/index/byte-bound verification,
tenant isolation, idempotent identical registration, immutable version conflict,
fail-closed canonical-byte corruption, missing-pinned-definition quarantine,
and a 24-way conflicting registration race with one durable winner. The 91
provider tests and twelve durable Runtime tests run independently against
PostgreSQL 16 and PostgreSQL 17.

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

Scheduler discovery first calls `load_runnable_run_page` for a tenant. The
first call fixes `snapshot_at` to the PostgreSQL transaction timestamp and
returns at most 16 complete `StoredRun` values ordered by
`(available_at, run_id)`; `available_at` is the later of durable queue entry and
lease expiry. Following the opaque `next_cursor` retains that cutoff, so newly
admitted or requeued work cannot move behind the cursor and an expired lease
cannot make a bounded scan chase time. Discovery is read-only and never grants
ownership. The scheduler selects an exact run, allocates a fresh `AttemptId`,
calls `claim_lease`, and treats `LeaseHeld` as ordinary contention. Cross-tenant
weighting and fairness remain scheduler policy rather than database queue
semantics; neither an empty page nor a lost claim means global work is absent.

## Transactional outbox dispatch

Register each immutable, tenant-owned destination revision with
`register_outbox_destination`. Its `OutboxDestinationRef::snapshot_digest`
must equal the canonical configuration digest. Store only protocol routing and
external credential handles; adapters resolve scoped secrets after claim, and
raw credentials, response bodies, or tokens never enter durable records.

Create stable `DeliveryId` values before calling
`append_control_plane_with_outbox` or `append_worker_with_outbox`. Every intent
must name that append's exact `EventId`; the batch is non-empty, duplicate-free,
and capped at 64. An uncertain enqueue is retried with the same event and
complete delivery set. A subset, changed payload/deadline/destination, or reused
delivery identity is a conflict rather than a second enqueue.

Dispatch workers allocate a fresh `AttemptId` and call
`claim_outbox_delivery(tenant, attempt_id)`. `Claimed` or live `Idempotent`
contains the validated destination, immutable delivery, and fixed
`DeliveryFence`; only then may the adapter perform network I/O. `NoWork` means
only that no eligible unlocked row was visible. The adapter's total request
timeout must be strictly shorter than `with_outbox_attempt_lease`, and that
lease cannot exceed five minutes or be renewed. A repeated claim after its
exclusive expiry fails instead of authorizing late dispatch.

Commit protocol-defined success through `acknowledge_outbox_attempt`, or a
public-safe `Failure` through `fail_outbox_attempt`. `SafeAfter` schedules from
the database completion time, while `Never` dead-letters. `ReconcileFirst` is
rejected because this queue deliberately supports only duplicate-tolerant
at-least-once notifications; model/tool side effects with ambiguous outcomes
stay in their dedicated ledgers. Losing an external acknowledgement can cause
the same `DeliveryId` and payload to be sent again, so a protocol adapter may
claim receiver-side deduplication only when its negotiated binding actually
carries and enforces that identity.

Audit code can call `load_outbox_delivery` and page
`load_outbox_attempt_history_page`. Each page request replays the complete
bounded predecessor history so a constructed cursor cannot hide corruption.
Terminal expiry and an unfinished 64th attempt are projected by a bounded
indexed reap step at claim time; no synthetic network outcome is written.

After a scheduler claims a candidate, construct one stable
`CorruptionQuarantineContext` from that candidate's exact journal head and call
`begin_claimed_run_recovery` with the returned `RunFence`. The resulting
`ClaimedRunRecovery` binds the context to that fence, verifies the live lease
with the database clock, and supplies scope-bound checkpoint-lineage, journal,
tool/model-history, node-attempt-history, and unconsumed-result pages. Start the
lineage without a cursor and follow each exact `next_cursor` until the
superstep-zero root. The first value is the current barrier observed in the
initial repeatable-read snapshot; immutable continuation cursors remain valid if
a later barrier advances the run between pages. Then page the journal strictly
after the trusted checkpoint/archive head. Call `revalidate` after deterministic
replay and before preparing durable work when consuming those pages manually.
This composes a corruption-isolating
checkpoint-and-suffix input without treating a read as dispatch authority;
node/model/tool/outbox start transactions still perform the decisive fence.

Before planning work, `load_pinned_graph` reloads the current checkpoint's exact
tenant-scoped graph version, recompiles its canonical bytes, validates the graph
and state-schema digest binding, rejects unknown ready nodes or an invalid
initial entry set, and repeats the live-fence/journal observation after the
registry read. `plan_ready_nodes` performs this pinned-graph check automatically.
A missing, altered, cross-tenant, or checkpoint-mismatched definition is treated
as corruption and enters the same fence-bound quarantine path; ordinary
availability and stale-fence races remain explicit errors.

For the current root ready set, prefer `plan_ready_nodes`. It loads the pinned
checkpoint, streams all unconsumed results and every ready activation's complete
history, and performs its own final live-fence/journal revalidation at database
time. Decisions are returned in canonical `NodeId` order:

| Decision | Runtime action |
|---|---|
| `Completed` | Reuse the immutable result; never run the node again |
| `Dispatchable` | Call `start_recovered_node_attempt`; only a newly `Committed` start grants this caller permission to launch node code |
| `Deferred` | Call `schedule_delayed_retry_wakeup` only when every sibling is `Completed` or `Deferred`; `Scheduled`/`Idempotent` relinquish ownership until `not_before`, while `Due` retains the lease for replanning |
| `InFlight` | Do not create another same-fence physical attempt |
| `Failed` | Surface the terminal public-safe failure; do not infer retryability |
| `Exhausted` | Surface hard attempt-limit exhaustion; no new physical start is legal |

The worker append passed to `start_recovered_node_attempt` must carry the same
fence and an exact journal expectation at or after the plan observation. For
multiple ready siblings, advance that expectation after every committed start.
A stale concurrent append is ordinary contention; reload the current head and
retry the intended sibling. Retrying the same `EventId` and node `AttemptId`
converges on the existing start, but an `Idempotent` outcome is not fresh
dispatch authority: it cannot distinguish a lost acknowledgement from a
concurrent executor and must be treated as in flight. If that executor is gone,
allow lease expiry/supersession and recover the unfinished start under a higher
fence. Never invoke node code before a fresh `Committed` handoff.

The unpublished `stateknot-runtime` crate now resolves an exact startup-frozen
schema/reducer/node executable closure, independently validates every committed
noninitial checkpoint, and drives the root recovery loop. It commits a physical
node start before calling node code, never launches after an `Idempotent` start,
refreshes a near-expiry lease before launch, renews it beneath a conservative
database-time-derived monotonic expiry watchdog, commits node completion against
the latest journal head, automatically commits Continue barriers, and returns
typed lease-bound handoffs for Wait/Terminal or blocked failure supervision.
The runtime lifecycle coordinator now consumes those handoffs, and the
tenant-scoped Agent Loop binds discovery, claim, Driver, lifecycle commit, and
cleanup. Cross-tenant fairness remains outside the provider; trusted terminal
admission/accounting evidence remains an application-owned durable boundary.

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
this orphan-prevention guard. A successor additionally requires the exact
pending-result barrier described below; the initial-checkpoint APIs cannot
bypass it.

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

Node execution first calls `start_node_attempt`; its event, run-wide physical
attempt claim, immutable start, and run head commit before user node code is
dispatched. `fail_node_attempt` appends one public-safe failure completion.
`succeed_node_attempt` atomically appends the completion and its immutable
`PendingNodeResult`, including exact committed tool/model bindings. The result's
semantic digest deliberately excludes physical ownership, but its stored row is
owned by the exact successful attempt. A changed activation input, state update,
control result, or invocation binding is a conflict. The legacy
`commit_pending_node_result` entry point rejects every call with
`NodeAttemptRequired`, so new code cannot fabricate a result without a durable
start. `load_node_attempt` and `load_node_attempt_history_page` verify canonical
bytes, redundant projections, base checkpoint, start/completion events, fences,
retry history, and successful result ownership. `load_pending_node_result`
fails closed unless the immutable row, owning attempt when present, base
checkpoint, worker event, all binding rows, full committed invocation records,
and their journal projections agree.

`append_control_plane_barrier` and `append_worker_barrier` accept a core
`CheckpointBarrier` only after full-record verification. Under the run lock they
compare the entire canonical compact set, reject missing or additional results
and unsettled invocations, and append one consumption row per result alongside
the exact successor. Reducers and graph callbacks run before this API; none
execute while a database transaction is open.

Waiting at a root checkpoint uses
`append_control_plane_initial_wait_checkpoint` or its fenced worker form.
Waiting after node execution uses `append_control_plane_wait_barrier` or
`append_worker_wait_barrier`; this consumes the exact complete pending-result
barrier and registers the full interrupt/timer set in the same transaction.
`resolve_interrupt` and `fire_timer` use the database clock and update exactly
one registration plus the lifecycle. A final resolution/firing requeues the run
as active. `append_*_abandon_waits` is mandatory when cancellation or failure
wins while waiting and writes complete append-only abandonment evidence.
`load_due_timer_page` and `load_expired_interrupt_page` use fixed database-time
cutoffs and bounded keyset cursors; callers still commit the returned item
through the exact terminal API. Immutable registration and terminal/audit loads
recheck canonical bytes, redundant projections, registration and terminal
journal anchors, and the current lifecycle wait-set projection.

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

Migration 5 adds immutable `pending_node_results`, separate tool/model binding
tables, and append-only `pending_node_result_consumptions`. Composite foreign
keys prove the exact base checkpoint, worker event/fence, logical invocation
activation, committed revision, and causal journal order without polymorphic
triggers. One logical `(tenant, run, base checkpoint, namespace, node)` key
admits one semantic result; changing the activation input is a conflict rather
than a second row. Recovery scans unconsumed rows through a two-record decoded
page bound and compact look-ahead. Its cursor pins the observed run journal head
so concurrent insertions cannot create keyset-pagination gaps. The consumption
table records one immutable mapping from every exact result to the immediately
following checkpoint. Barrier-event idempotency binds both the lifecycle
projection and the core barrier intent digest.

Migration 6 extends `run_attempt_claims` with exact node-start ownership, creates
immutable `node_attempts` and append-only `node_attempt_completions`, and adds an
optional attempt owner to pending results. New successful results require that
owner and are committed with the completion. Existing migration-5 rows retain a
null owner because no historical start can be reconstructed truthfully; their
original event/projection integrity remains fully verified. The migration and
its migration-5 upgrade fixture are checksum-pinned on PostgreSQL 16 and 17.

Migration 7 adds the validated `scheduler_ready_at` projection and its partial
effective-availability index. It backfills pending and leased runs from durable
timestamps without inventing a scheduler observation. Fixed-snapshot paging,
constraint removal, and exact v6 upgrade behavior are tested on PostgreSQL 16
and 17.

Migration 8 adds `outbox_destinations`, `outbox_deliveries`, immutable
`outbox_attempts`, and append-only `outbox_attempt_completions`. It extends the
existing run-wide claim registry with an exact outbox owner while preserving
the original one-event/one-non-outbox-attempt anchor rule through a partial
unique index. Composite foreign keys bind delivery→journal/destination,
attempt→delivery/claim, completion→start, and mutable current projections back
to their immutable evidence. Ready, expiry, abandoned-limit, and origin indexes
serve bounded operational queries. The exact v7 registry upgrade and existing
node-attempt readability are exercised on PostgreSQL 16 and 17.

Migration 9 adds the run wait-set digest/count/deadline projection, canonical
`run_wait_registrations`, append-only `interrupt_resolutions`, `timer_firings`,
and `wait_abandonments`, deferred terminal back-references, and partial due and
expiry indexes. Evidence-free waiting rows from migration 8 are preserved but
quarantined; the migration never invents payload, authority, deadline, or
journal evidence. Fresh install, exact v8 upgrade, index plan, schema removal,
and full transaction behavior are checksum-pinned and exercised on PostgreSQL
16 and 17.

Migration 10 adds `run_quarantines`, an immutable tenant/run-scoped observation
outside the journal it may report as corrupt. New evidence binds a stable
`QuarantineId`, closed cause, bounded non-secret component, caller-retained
evidence digest, exact empty/current journal observation, database time, and a
canonical record digest. The same transaction clears any active lease and sets
the existing quarantine projection, which removes the run from the partial
runnable index. Existing quarantined rows remain untouched and deliberately
have no fabricated structured evidence.

Migration 11 adds an optional exact recovery `AttemptId` and `FencingEpoch` to
that evidence. Fenced records use a versioned v2 digest; existing unfenced rows
retain their v1 digest and nullable columns. The quarantine transaction locks
the run, checks that exact fence against an unexpired database-clock lease, and
repeats the predicate in the projection update that revokes ownership. Lost-ACK
retries still resolve by the same immutable quarantine ID after the lease has
been cleared.

Migration 12 adds nullable `scheduler_not_before` while preserving migration
7's `scheduler_ready_at` queue age. Its validated shape constraint permits a
gate only on an unleased runnable projection and never before queue admission.
The stable `runs_scheduler_ready` index now orders by the greatest of queue admission,
retry gate, and lease expiry. `schedule_delayed_retry_wakeup` accepts only an
exact deferred-only recovery plan (completed siblings are allowed), locks and
revalidates all plan anchors, then stores the earliest deferred boundary and
clears the lease in one transaction. An identical retry after an ambiguous
commit returns `Idempotent`; if database time already reached the boundary,
`Due` leaves the lease untouched for replanning. Exact v11 upgrade, constraint
removal/corruption, direct early claims, indexed due visibility, lost-ACK, and
the due-during-commit race are exercised on PostgreSQL 16 and 17.

Migration 13 adds immutable `graph_definitions` keyed by tenant and exact
owner-qualified semantic graph identity, plus a tenant/digest lookup index.
Canonical compiled bytes remain the authority; bounded redundant key and digest
columns support lookup and are revalidated on every load. `register_graph_definition`
uses conflict-safe idempotency, while `load_graph_definition` recompiles the
closed descriptor and rejects any projection or canonical-byte drift. Claimed
recovery maps a missing or contradictory checkpoint-pinned definition through
its existing live-fence quarantine transaction. Fresh install, exact v12
upgrade, constraint/index removal, tenant isolation, corruption, conflict, and
24-way registration races are exercised on PostgreSQL 16 and 17.

## Not yet implemented

This slice is not a production release or the complete agent runtime. It does
not yet implement protocol-specific outbox dispatch adapters, artifacts,
cross-tenant scheduler fairness, first-party model/tool Agent ergonomics,
retention/archive/legal hold, backup/restore, failover qualification, or the
10,000-race stale-worker gate. The implemented lifecycle coordinator now
atomically commits complete Wait/success/failure handoffs, and the tenant worker
binds runnable discovery, lease claim, Driver, and lifecycle coordination into
one bounded Agent Loop quantum. It still requires the embedding service to
supply trusted durable admission and cumulative-accounting evidence; it never
guesses missing values. The current Graph Driver is deliberately
sequential within one run so exact journal serialization and recovery authority
remain unambiguous; parallel sibling scheduling requires its own bounded
ordering and admission policy before it can be enabled.

The current pool is a trusted server-side persistence boundary. Database
credentials must not be distributed to untrusted workers: PostgreSQL
stored-procedure/role separation and the final control-plane/worker service
boundary remain required by RFC-0003. Passing the current integration tests does
not establish an RPO/RTO, multi-replica, security-role, or production-support
claim.
