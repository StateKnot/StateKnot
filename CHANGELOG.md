<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Changelog

All notable changes to StateKnot will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and released versions will follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Failure-close source COMMIT-loss and expired-fence qualification: a bounded,
  loopback-only, single-session PostgreSQL test proxy holds the COMMIT request or
  withholds both successful commit response frames before actual client SIGKILL.
  Independent reads verify rollback or original-decision recovery; a retained old
  process is rejected with `StaleFence` after real database-clock lease expiry and
  higher-epoch takeover. Includes exact journal/accounting recovery, process
  resume/parser safety smoke tests, bilingual guides and mandatory PostgreSQL
  16/17 CI evidence. Only Tokio test I/O features are enabled; no dependency
  version, lockfile, production API/migration/schema or website changes. Other
  transaction/provider/failover/role-isolation/capacity gates remain open.

- Executable six-point committed-boundary failure-close OS-kill qualification:
  independently restarted processes, actual Unix SIGKILL/reaping, bounded
  readiness, exact lifecycle/journal/receipt/account/settlement checks, original
  failure preservation and once-only 7 + 11 token accounting. PostgreSQL 16/17
  CI retains machine-readable phase evidence and source/environment metadata;
  bilingual guidance defines exclusions. No production API, migration or
  dependency changes; pre-/in-commit, provider-effect, role-isolation,
  failover/restore and full-profile capacity gates remain open.

- PostgreSQL migration 24 and `DurableRunFailureCloser`: immutable original failure
  decisions for Active parents with complete priced direct evidence, atomic child
  cancellation capture and parent lease release, and bounded tenant maintenance
  that commits the original Failed outcome with once-only child accounting.
  Child-enabled graph lifecycle/Agent Loop handoffs expose `FailureClosing`;
  ordinary graph failure behavior and existing lifecycle wire formats remain
  unchanged. Includes source/final rollback, lease-expiry, unknown usage,
  reconstructed handoff/child-close recovery, pagination/quarantine/tenant,
  populated v23 upgrade and exact schema-drift tests, plus bilingual host guidance.
  Existing migrations 1–23 and schemas are unchanged. Direct uncertain effects
  must be recovered before registration; complete-profile process-kill, role
  isolation, failover/restore and measured capacity qualification remain gated.

- PostgreSQL migration 23 and `DurableAgentDeadlineReconciler`: indexed admitted
  root/child deadlines, immutable admission projection/backfill, exact startup
  guards and bounded tenant-scoped scans. Expiry is rechecked with the database
  clock under the Run lock; cancellation, wait abandonment and child queue capture
  commit atomically. Includes real-store clock/race/rollback/restart/Join-drain,
  quarantine, pagination, populated v22 upgrade tests and bilingual host guidance.
  Migrations 1–22, existing wire contracts and audit schemas are unchanged. No
  fabricated terminal usage, hard-kill/SLA claim, automatic failure-close intent
  or general child-profile enablement.

- Opt-in exclusive Graph Driver child Join suspension/resumption, schema-validated
  lazy slot access and automatic parent-result consumption, plus a tenant-scoped
  bounded `DurableChildJoinPublisher` with an independent pinned offline schema.
  Includes real-store independent child execution/restart/accounting, rollback,
  cancellation, publication pagination/error recovery, late operator-quarantine
  read/consumption guards and pre-dispatch timeout
  qualification. Bilingual host integration guidance distinguishes remaining
  deadline/failure-close and full-profile gates. Published migrations 1–22 and
  existing protocol schemas are unchanged. Pre-alpha source change:
  `GraphNodeExecution::new` remains; unconditional getters/`into_parts` become
  explicit matching on `Completed { .. }` versus `ChildJoin`. No automatic state
  merge, fake wait/failure/usage, or general-profile website enablement.

- PostgreSQL migration 22 and bounded core Join contracts: sealed complete child
  membership, atomic registration/parent lease release, indexed ready discovery,
  exact terminal publication/scheduler wakeup and once-only parent-result
  consumption. Adds mixed-binary/closure guards, canonical evidence verification,
  populated v21 upgrade, concurrent/rollback/cancellation/replay tests and bilingual
  operations guidance. Existing no-Join result bytes and migrations 1–21 stay
  unchanged. Automatic Graph Driver Join control/context and publication worker,
  failure-close/deadline policy and full-profile enablement remain unshipped.

- PostgreSQL migration 21 and a bounded `DurableChildReconciler`: atomic parent
  cancellation capture/backfill, child request/real-wait cleanup/descendant queue
  delivery with immutable recovery receipts, and restartable cancellation and
  settlement pages that continue past unresolved items. Cancelling parents cannot
  acquire new leases before child settlement; existing cleanup leases can renew.
  Includes a pinned offline audit schema, compiled usage example, bilingual
  operations guidance and PostgreSQL 16/17 race/fault/upgrade tests. No fabricated
  terminal usage or force termination. Dedicated Join, deadline/failure close
  policy and successful parent suspend/resume remain unshipped (RFC-0004 Draft).

- PostgreSQL migration 20: atomic isolated child admission/ownership and cumulative
  reservation; ancestor topology limits, immutable terminal notification/settlement,
  bounded resumable discovery, mixed-worker capability gates and centralized parent
  closure/checkpoint guards. Runtime model/tool starts deduct child charges and pin
  the account digest; terminal success/failure/cancellation include delegated usage.
  Includes fault-injection and retry/concurrency tests. Automatic Join and
  successful parent resumption remain unshipped (RFC-0004 stays Draft).

- Core child-budget account state transitions with separate direct observations,
  outstanding ceilings and exact immediate-child subtree settlements. Frozen
  versioned snapshots retain retry identities, refuse regressing direct evidence,
  preserve known overruns, and keep unknown-price reservations unsettled.
  Includes bounded decoding, canonical integrity fixtures, property tests and
  bilingual guidance. These are pure contracts, not a durable ledger, child
  execution API or a replacement for transactional direct-work enforcement.
- Graph-pinned version-one child delegation policies with exact Agent definition,
  graph and I/O schema pins, bounded node-owned slots, explicit topology ceilings,
  and startup target/depth closure. Runtime preparation now rejects undeclared
  or substituted targets. Existing graph digests are preserved; static expansion
  cannot silently discard declarations. Includes strict wire and real-store
  tests, an offline example, and bilingual contract guidance. Durable child
  Join and automatic successful parent resumption remain unshipped.

- Pinned child admission preparation with candidate-ID-independent retry
  fingerprints, exact parent/checkpoint binding, same-principal scope narrowing,
  all-dimension immutable budget narrowing, and offline child graph/schema
  validation. Includes read-only runtime preparation/revalidation, compiled
  documentation, and real PostgreSQL no-write/cancellation/noninitial tests.
  Automatic child joins remain unshipped.
- Core child ownership keys with strict, digest-checked wire restoration and
  retry/fence-independent logical identity; bounded cumulative budget
  reservation arithmetic separating high-water topology from expenditure.
  Includes an offline example, bilingual contract notes, and RFC-0004. These
  primitives do not yet implement durable child admission, joins, or lifecycle.
- Static shared-state subgraph composition and finite loop expansion into the
  existing durable graph contract, with exact schema/reducer pins, scoped
  node/route identities, preserved source ordering, explicit return/exhaustion
  continuations, static nesting, and bounded compilation. Includes a runnable
  executable-registry example, bilingual guides, and PostgreSQL recovery tests.
- Pre-dispatch global graph superstep enforcement, including recovered runs at
  the limit, with exact-usage lifecycle supervision and idempotent terminal
  failure instead of dispatching excess work or repeatedly retrying a barrier.
- Bounded durable model-native structured-output repair with distinct invocation
  and attempt identities, exact usage accounting, crash-safe replay, reserved
  trusted instructions, retained completed Tool history with new calls disabled,
  first-party provider `none` selection contracts, and explicit exhaustion.
  PostgreSQL migration 19 allows node results to consume exact known-failed
  model revisions while rejecting unfinished outcomes. Includes bilingual
  integration guides and PostgreSQL 16/17 qualification cases.
- Provider-neutral durable Tool reconciliation SPI with original-attempt
  identity, finite deadline/cancellation context, bounded `Pending` polling,
  public-safe probe failures, pre-I/O ledger reload, and atomic schema-checked
  result/error commits that converge without repeating provider I/O.
- Provider-native Agent automatic `Unknown` recovery through durable
  `SafeAfter` node retries, using a deterministic reconciliation audit event
  derived from the existing immutable Tool plan so checkpoint wire and digest
  compatibility remain unchanged.
- A2A operator-attested context/task-history reconciliation and exact
  message-ID replay modes, with opaque local context correlation, bounded
  pagination/history scans, attempt-scoped authorization, fail-closed duplicate
  and same-ID payload-substitution detection, no blind resend, A2A loopback
  contract tests, and separate provider-neutral PostgreSQL Agent Loop evidence
  proving one business call across pending recovery.
- A2A 1.0 HTTP+JSON and JSON-RPC/SSE Server profile with StateKnot-owned bounded
  Agent Card, message, task, artifact, stream, and push contracts; exact
  Host/Origin/route/version/extension enforcement; authentication before body
  parsing; authorization before lookup; process and replica admission; bounded
  responses; Agent Card caching; and cooperative shutdown.
- Frozen official A2A TCK commit and archive checksum, audited upstream harness
  patch, deterministic full-capability fixture, independent result-drift
  verifier, retained CI evidence, and mandatory 265-case gate: 177 pass, 88
  declared skips, zero failures/errors/xfails, with critical cases required to
  execute.
- English and Simplified Chinese A2A Server production guide, exact conformance
  disclosure, site routes, navigation, status updates, and browser contracts.
- Separate general stateless MCP 2026-07-28 Tool client with bounded discovery
  and pagination, JSON/request-scoped SSE, standard and nested custom headers,
  invalid-Tool isolation, request-scoped authorization, exact MRTR request
  state, hard transport ceilings, and no network schema dereference.
- Pinned official MCP client runner and mandatory CI gate for all seven scored
  non-OAuth scenarios in the frozen 2026-07-28 requirement set: 45 successful
  assertions, zero failures, 11 explicit out-of-surface skips, and no
  expected-failures baseline.
- Bilingual general MCP client tutorial and updated conformance evidence,
  implementation-status, roadmap, navigation, responsive tables, and browser
  route/accessibility contracts.
- Public `AgentServiceV1` embedding boundary with authorization-before-lookup,
  immutable deployment registration, durable submission-key recovery, exact
  cancellation identities, database-authoritative timestamps, and a stable
  versioned control-event schema published identically by the runtime and site.
- Strict MCP 2026-07-28 Remote Tool integration pinned to the official Rust SDK,
  with exact server/tool/schema identities, bounded stateless JSON transport,
  fail-closed capability discovery, trusted local policy metadata, and explicit
  reconciliation-first handling for ambiguous external writes.
- English and Simplified Chinese AgentService and MCP integration guides, site
  navigation, implementation-status disclosures, and browser contract tests.
- Provider-neutral durable model/tool attempt execution with immutable
  exact-version provider registries, trusted budget and paired-clock admission,
  durable-before-dispatch starts, unary and durably-sunk streaming models,
  reconciliation-safe tool cancellation/deadline handling, bounded lost-ACK
  retries, and retained no-dispatch terminal recovery handoffs.
- Replica-safe cross-tenant smooth weighted scheduling with immutable
  shard-scoped policies, globally ordered PostgreSQL reservations, exact
  per-cycle shares, explicit reservation-count starvation bounds, bounded
  database-time retention, property tests, and PostgreSQL 16/17 concurrency and
  runtime qualification.
- A strict public-safe invocation execution event schema plus bilingual
  production integration guides for durable invocation execution and
  cross-tenant fair scheduling.
- New unpublished `stateknot-runtime` crate with immutable, digest-pinned,
  offline JSON Schema 2020-12 validation and a startup-frozen executable graph
  registry that requires complete graph/reducer/node/schema closure and rejects
  conflicting or orphan code bindings.
- Full checkpoint-state validation and noninitial transition replay in the
  PostgreSQL claimed-run recovery surface, including bounded historical-result
  materialization, verified consumption rows, pure reducer/schema execution
  outside database transactions, exact successor comparison, final
  fence/journal revalidation, and fenced corruption quarantine for semantic
  divergence.
- Fenced durable Graph Driver with durable-before-dispatch node starts,
  acknowledgement-safe mutation retries, exact takeover semantics, bounded
  execution quanta, automatic Continue barriers, typed lifecycle/blocking
  handoffs, delayed-retry scheduling, cooperative shutdown and deadlines, plus
  a database-time-derived monotonic lease watchdog that prevents node launch
  under a near-expiry or expired idempotent renewal.
- Apache-2.0 bilingual durable-runtime integration and operations guides,
  English/Chinese website route parity, and a digest-identical immutable public
  Graph Driver journal-event schema served as `application/schema+json`.
- Bilingual English and Simplified Chinese public documentation with localized
  route parity, explicit language switching, canonical and `hreflang` metadata,
  localized search, and browser gates for links, accessibility, responsive
  layout, contrast, copy feedback, and error templates.
- Initial Rust workspace and repository governance.
- Architecture research, implementation plan, completeness audit, and roadmap.
- Frozen v1 scope and production qualification scenarios with measurable load,
  failure, recovery, security, and interoperability gates.
- Initial RFC draft for the core domain, typed capability, identity, budget,
  error, and canonical serialization contracts.
- Initial `stateknot-core` implementation with validated tenant identifiers and
  canonical, strongly typed UUIDv7 identifiers.
- Canonical three-component contract versions and SHA-256 integrity digests,
  including strict Serde/JSON Schema validation and external compatibility
  fixtures.
- Canonical UTC microsecond timestamps and checked, non-negative millisecond
  durations with strict precision-preserving standard-library conversions and
  exact decimal-string JSON encoding.
- Checked token/byte counters and exact micro-unit money with strict currency,
  overflow, cross-currency, Serde, schema, property, and fixture validation.
- Normalized, offline-only HTTPS schema identifiers and immutable
  ID/version/digest schema references.
- MCP-compatible capability names plus bounded OAuth-compatible scopes and
  deterministic, duplicate-rejecting scope sets with narrowing property tests.
- Exact OIDC/OAuth issuer and redacted subject identifiers composed into a
  strict principal identity key, without unsafe URI normalization.
- Streaming bounded JSON materialization with immutable hard ceilings,
  decoded duplicate-key rejection, exact compact-size accounting, redacted
  diagnostics, property tests, and cross-version fixtures.
- Bounded text and structured content with stable RFC 5646 language tags,
  opaque security labels, explicit source/trust/redaction metadata, redacted
  diagnostics, closed schemas, exhaustive Unicode checks, and versioned wire
  fixtures.
- Tenant-scoped immutable artifact references with canonical RFC media types,
  bounded path-safe presentation metadata, integrity and schema bindings,
  explicit retention/provenance/lineage, closed content-part envelopes,
  redacted diagnostics, and versioned wire fixtures.
- Integrity-bound application instructions separated from provenance-bound
  user/assistant/tool messages, with strict producer/source-role matrices,
  aggregate inline payload limits, redacted diagnostics, closed schemas, and
  versioned wire fixtures.
- Finite layered execution budgets with 21 explicit dimensions, checked
  monotonic/high-water usage, normalized token subsets, bounded multi-currency
  cost ceilings, fail-closed unknown pricing, exact remaining-capacity
  evaluation, closed schemas, and versioned wire fixtures.
- Protocol-neutral failures with UUIDv7 occurrence identity, closed semantic
  categories, stable code/origin identifiers, bounded public-safe messages and
  schema-bound details, explicit retry/reconciliation advice, non-serializable
  private source chains, closed schemas, and versioned wire fixtures.
- Sorted, duplicate-rejecting namespaced extensions with canonical HTTPS/URN
  and strict reverse-DNS identities, explicit opaque/schema-bound trust modes,
  exact compact-map accounting, immutable hard ceilings, caller-only
  narrowing, redacted diagnostics, closed schemas, and versioned wire fixtures.
- Owner-qualified, version-pinned common capability metadata with bounded
  redacted discovery text, closed kinds, validated active/deprecated/retired
  lifecycles, self-replacement rejection, required scopes, bounded extensions,
  closed schemas, property tests, and versioned wire fixtures.
- Endpoint-bound model capabilities with sorted multimodal input/output sets,
  schema-profiled and finitely bounded tool calling, explicit strict/choice/
  parallel semantics, tiered structured output, readable reasoning summaries,
  fail-closed unknown token ceilings, exhaustive requirement mismatch reports,
  closed schemas, property tests, and versioned wire fixtures.
- Immutable model descriptors that bind owner-qualified, version-pinned common
  metadata to one validated capability snapshot, reject non-model metadata and
  keep mutable provider/endpoint bindings behind the trusted registry identity,
  preserve redacted discovery diagnostics, and publish an independent versioned
  wire fixture without mutating prior fixtures.
- Immutable provider-neutral model requests with ordered bounded instructions,
  durable multimodal messages, canonical pinned tool descriptors, exact tool
  selection and call ceilings, structured text output, complete/streaming and
  readable-reasoning controls, finite token/content limits, automatically
  derived capability requirements, tamper-resistant deserialization, redacted
  diagnostics, closed schemas, property tests, and an independent versioned
  wire fixture.
- Immutable provider-neutral model responses with attempt/model provenance,
  ordered typed content, readable reasoning summaries, unapproved exact-identity
  tool proposals, closed portable finish reasons, inclusive per-attempt token
  accounting, request/descriptor binding, strict structured-output and modality
  checks, aggregate resource ceilings, redacted provider identifiers and
  diagnostics, closed schemas, property tests, and an independent versioned
  wire fixture.
- Bounded provider-neutral model streaming with contiguous per-attempt semantic
  sequences, typed output headers and exact text/JSON/tool deltas, interleaved
  ordered outputs, cumulative monotonic usage, authoritative terminal events,
  a permanently poisoning response accumulator, strict EOF/resource handling,
  convergence to the unary `ModelResponse` contract, closed schemas, property
  tests, and an independent versioned wire fixture.
- Runtime-neutral callable model contracts with object-safe unary/streaming
  dispatch, executor-independent boxed futures and streams, capability-limited
  attempt contexts, paired durable/monotonic deadlines, cooperative cancellation,
  provider request correlation, phase-aware public-safe failures, explicit
  hidden-retry prohibition, closed schemas, compile-time boundary tests, and an
  independent versioned wire fixture.
- Production-shaped callable tool contracts with a strongly typed authoring API,
  object-safe erased dispatch, trusted offline schema-registry gate, frozen
  descriptors, logical invocation and physical attempt identity, stable derived
  idempotency keys, intersected durable/monotonic deadlines, bounded inline JSON
  and artifact references, tenant/run/tool provenance checks, finite ordered
  progress reporting that poisons on concurrency gaps, dropped futures, or sink
  failures, explicit external side-effect evidence, reconciliation-safe failures,
  hidden-retry prohibition, redacted diagnostics, and an independent versioned
  wire fixture.
- Immutable agent definition snapshots binding exact input/output schemas, a
  pinned model, ordered application-controlled instructions, canonical resolved
  tools, a resolved native-or-tool-call structured-output strategy, finite
  model/repair/tool-call limits, deterministic sequential or read-only parallel
  scheduling, reusable budget layers, model-capability preflight, reserved-name
  protection, redacted diagnostics, closed schemas, property tests, and an
  independent versioned wire fixture.
- Runtime-neutral agent admission and successful-result contracts with
  schema-bound bounded input/output, request-local restrictive budget layers,
  deterministic full-budget resolution, new-admission retirement fencing,
  tenant/run/thread/invocation/agent provenance, bounded final artifact
  references, cumulative usage reconciliation, completion-time enforcement,
  redacted diagnostics, closed schemas, adversarial mutation coverage, and an
  independent versioned wire fixture.
- A protocol-neutral durable run lifecycle with typed optimistic revisions,
  pending/active/waiting/cancellation-requested and exclusive terminal states,
  bounded multi-interrupt and durable-timer waits, strict expiry/firing rules,
  two-phase cancellation precedence, immutable terminal usage/failure records,
  closed schemas, randomized model testing, and an independent versioned wire
  fixture. Worker attempts, leases, and fencing remain separate runtime state.
- Explicit RFC 8785 canonical JSON with fail-closed I-JSON integer validation,
  plus PostgreSQL-compatible journal sequences and fencing epochs, run-scoped
  attempt tokens, exclusive-expiry lease renewal/supersession, schema-bound
  canonical event payloads, stable EventId append intents, exact-head optimistic
  requests, payload/intent/event digest layers, streaming hash-chain validation,
  closed schemas, randomized state tests, and an independent versioned wire
  fixture. The database-neutral types do not claim authority without the
  conditional PostgreSQL transaction specified by draft RFC-0003.
- Draft RFC-0003 defining the PostgreSQL append transaction, idempotency order,
  database-clock lease fencing, record model, recovery/quarantine, retention,
  migration, backup/restore, security, and release evidence required before the
  durability layer can be called production-ready.
- Draft RFC-0002 plus database-neutral immutable graph checkpoint contracts for
  bounded supersteps, stable node identities, deterministic ready sets,
  graph/state-schema pins, exact parent and journal heads, RFC 8785 state, and
  domain-separated state/intent/checkpoint integrity.
- Initial unpublished `stateknot-store-postgres` slice for PostgreSQL 16/17,
  with exact checksum-pinned migration verification, strict runtime startup,
  secure TLS defaults, bounded pools/transactions, tenant-scoped admission,
  canonical journal persistence, locked pure lifecycle transitions, atomic
  event/head/projection commits, complete-cursor paging, and fail-closed decode.
- Database-clock lease claim/renew/release/supersession with stable-attempt
  idempotency, monotonic fencing epochs, exact worker predicates on event and
  run-head writes, lost-ack convergence, post-insert rollback injection, and
  digest-pinned PostgreSQL 16/17 CI covering 100 concurrent appenders.
- Immutable PostgreSQL checkpoint persistence with projection-bound journal
  idempotency, exact parent and journal anchoring, atomic lifecycle/event/
  checkpoint/head commits, fenced worker writes, fail-closed recovery reads,
  v1-to-v2 data migration coverage, injected rollback/corruption tests, and 24
  concurrent checkpoint writers on PostgreSQL 16/17.
- Streaming reverse checkpoint-lineage verification plus bounded PostgreSQL
  repeatable-read pages with exact continuation heads, batched fully decoded
  journal-anchor checks, later-barrier safety, and fail-closed cursor/ancestor
  corruption coverage on PostgreSQL 16/17.
- Durable tool-invocation intents and hash-linked revision state machines with
  exact checkpoint/node/journal ownership, stable logical and physical attempt
  identities, prepared/executing/committed/failed/unknown outcomes,
  reconciliation-only ambiguity, evidence-gated delayed retries, provenance and
  output-limit validation, redacted diagnostics, closed schemas, and a versioned
  canonical history fixture.
- Atomic fenced PostgreSQL tool-invocation preparation, transition, current-load,
  and bounded history APIs with exact lost-ack convergence, root ready-node and
  run-lifecycle admission, database-enforced attempt uniqueness, rollback and
  corruption injection, cancellation-race coverage, checkpoint advancement
  rejection while an invocation remains unsettled, and 24-writer single-winner
  tests on PostgreSQL 16/17.
- Durable model-invocation intents and compact hash-linked revisions with exact
  descriptor/request/response/error provenance, fresh physical attempts,
  database-clock-compatible delayed retry evidence, closed schemas, complete
  history verification, and a versioned canonical fixture.
- Atomic fenced PostgreSQL model-invocation prepare/advance/load/history APIs,
  exact lost-ack and cancellation-race behavior, checkpoint guards, rollback
  and corruption rejection, delayed-retry and 24-writer tests, plus migration 4
  with a run-wide tool/model attempt registry, v3 tool-attempt backfill, and
  exact kind/invocation/revision foreign keys verified on PostgreSQL 16/17.
- Immutable pending node-result contracts with exact logical activations,
  schema-pinned bounded updates and terminal output, stable conditional route
  identities, non-empty durable waits, canonical committed tool/model bindings,
  semantic idempotency separated from physical worker fencing, strict journal
  causality, redacted diagnostics, closed schemas, adversarial tests, and a
  versioned canonical-wire digest fixture.
- Integrity-bound checkpoint-barrier inputs that require exact root ready-set
  coverage, canonical result ordering, one immutable base checkpoint, an exact
  successor write, closed schemas, and frozen intent/wire digest fixtures.
- Durable core node-attempt starts and append-only completions with a physical
  node `AttemptId` distinct from the authorizing worker `RunFence`, exact
  activation/start/completion journal binding, atomic success references,
  public-safe failure causation, explicit delayed retry, higher-epoch crash
  takeover, absorbing success/non-retryable failure, redacted diagnostics,
  closed schemas, and frozen success/failure wire fixtures.
- PostgreSQL migration 6 and atomic node-attempt start/fail/succeed/load/history
  APIs with durable-before-dispatch starts, append-only completion evidence,
  database-clock delayed retry, higher-fence abandoned-work takeover,
  run-wide node/tool/model attempt identity, fail-closed canonical/projection/
  journal recovery, bounded history pages, exact lost-ack convergence, and
  attempt-owned pending results committed with successful completions. Existing
  migration-5 results remain readable without fabricated physical provenance;
  direct result writes that bypass an attempt now fail closed.
- Deterministic root ready-node activation derivation plus a bounded recovery
  planner that reuses immutable results, verifies complete physical histories,
  binds decisions to an exact checkpoint/fence/journal/database time, exposes
  completed/dispatchable/deferred/in-flight/failed/exhausted states, and
  enforces a 64-attempt ceiling. PostgreSQL claimed recovery builds and
  corruption-quarantines that plan, while its plan-scoped start transaction
  grants launch authority only for a fresh durable commit; PostgreSQL 16/17
  coverage includes crash takeover, result reuse, drift rejection, lost-ACK,
  24-way single-commit convergence, and the no-residue hard-limit boundary.
- PostgreSQL migration 12 and a plan-bound delayed-retry scheduler handoff that
  separates preserved queue age from an inclusive durable not-before gate,
  atomically releases the exact live fence, blocks direct early claims, becomes
  index-visible without a polling write, converges lost acknowledgements, and
  retains ownership when the retry becomes due during commit. Exact v11
  upgrade, constraint corruption, due-race, and scheduler visibility pass on
  PostgreSQL 16/17.
- PostgreSQL migration 5 and atomic pending node-result commit/load APIs with
  immutable canonical records, exact base-checkpoint and worker-event anchors,
  separate tool/model composite foreign keys for activation-bound committed
  revisions, semantic idempotency across lease takeover, fail-closed full-record
  recovery with one-model/two-tool/eight-anchor memory batches, cancellation
  and corruption coverage, complete rollback after an invalid binding, and
  24-writer single-winner tests on PostgreSQL 16/17, plus two-record
  stable-snapshot unconsumed-result pages whose complete cursor rejects
  concurrent journal advancement rather than skipping lower sort keys. The
  append-only consumption schema.
- Atomic PostgreSQL checkpoint-barrier APIs that verify full immutable inputs
  outside the run lock, recheck the exact complete compact result set under the
  lock, and commit the event, successor checkpoint, append-only consumption
  rows, lifecycle projection, journal head, and checkpoint pointer as one
  fenced transaction. Raw successor-checkpoint writes now fail closed;
  PG16/17 coverage includes lost acknowledgements after lease takeover,
  incomplete/conflicting sets, unsettled invocations, injected rollback, and
  24-way single-commit and linear-chain races.
- Protocol-neutral tool descriptors with digest-pinned schemas, closed and
  cross-validated side-effect/idempotency semantics, non-granting resource
  requirements, bounded cancellation/progress behavior, finite input/output/
  artifact/concurrency/time ceilings, redacted diagnostics, closed schemas,
  property tests, and versioned wire fixtures.
- CI, dependency policy, issue forms, and security reporting guidance.
- Crate package metadata and file lists that retain the Apache-2.0 SPDX
  expression while embedding `LICENSE`, `NOTICE`, and `README.md` in every
  distributable source archive.

### Changed

- The minimum supported Rust version is now 1.88.0, matching the supported
  compiler floor of the pinned MCP Rust SDK dependency.

### Fixed

- PostgreSQL CI now serializes top-level tests that intentionally share one
  migrated schema, while preserving each scenario's internal multi-threaded
  concurrency pressure and eliminating unrelated Tokio-runtime starvation.
- Concurrent identical graph registrations now converge through every unique
  index arbiter before verifying the immutable stored definition, instead of
  leaking a PostgreSQL `23505` race from the redundant exact-reference index.
