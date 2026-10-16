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

- Add the numeric-corrected experimental explicit MCP host-bound state
  profile for private cursors and real MRTR consumers on composite/direct services.
  Bind parsed argument values with sorted serde_json encoding and strict operation
  ADv3; cursorv2/envelope1 and unconfigured legacy semantics remain. Preserve host
  authority, independent Public Resource contents and all original limits.
  Fixed928-file source passes six local gates (2,098 workspace tests/6 original
  ignored,48doctests); both PostgreSQL majors pass all15 serial ingress cases with one marker each. See the
  [bounded record](docs/qualification/experimental-mcp-lossless-bound-state-2026-10-16.md).
  Historical14-case greens are retained with the actual numeric counterexample,
  which prevents adopting the old exact-argument candidate. Ubuntu/test-opt1 MCP,
  complete CI, deployment/backend, stdio and RFC-0025/R1 remain open.

- Expose immutable actual HTTP method, URL/query and bounded request body to
  A2A bearer-token providers before authenticated unary, streaming and recovery
  dispatch. Policy inspects the prepared request that is subsequently sent;
  new URL/body Debug fields are redacted. Existing providers and Anonymous keep
  their explicit behavior: request facts do not automatically authorize input.

- Authenticate experimental ordinary Root Continue/Wait/Terminal dispositions
  with the positive original Start and INITIAL checkpoint source. Schema 35
  preserves Agent V1 facts and complete owning verification while journal-binding
  trusted V2 creation and original terminal validation inputs. Fresh Root work
  verifies the original recovery observation before dispatch; deployment drains
  schema-34 pools before the reviewed migration and matching role profile.
  Final composed Native, compatibility and production gates remain open.

- Preserve complete trusted terminal validation inputs in the experimental
  `AgentTerminalValidationInputs` snapshot. Restored results use the same
  predicate and error order as `AgentResult::validate_for`, with provenance
  authenticated by the original Run. Closed readers, schema pins and the shared
  fuzz corpus cover the new snapshot. Ordinary Root persistence and final
  combined qualification remain under Draft RFC-0022.

- Drive authenticated nested graph frames with complete Entry/Rebind/barrier/
  Return transactions and one shared durable-event quantum. Trusted providers
  must supply exact-context complete DIRECT usage; the default is unavailable.
  `FrameWaiting` releases ownership with the scoped Wait and resumes through
  authorized resolution and a fresh claim. Draft RFC-0022, scoped child Join and
  full combined fault, compatibility and production qualification remain open;
  these APIs are not included in `0.1.0-alpha.1`.

- Prepare frame-owned isolated children with the complete scoped checkpoint and
  reserve them in the existing Root-bound Run account. Store admission and Join
  authenticate the actual leaf, original ownership namespace and declared
  policy under existing locks; scoped limits only narrow Root topology. Join
  excludes only authenticated suspended framework callers and still rejects
  unfinished ordinary siblings. Nested Driver and full fault qualification
  remain gated by Draft RFC-0022.

- Record Draft RFC-0024 for complete owned MCP stdio sessions: bounded catalog
  admission and correlation, per-call send phases, cancellation and process-tree
  cleanup, credential scope and durable uncertain effects. Platform owners,
  final limits, compilable public types and external-consumer qualification
  remain acceptance requirements; stdio is not implemented by this proposal.

- Record Draft RFC-0025 for separate MCP/A2A external identity, local tenant
  authority, destination-scoped credentials, input release and authorized
  recovery. It records actual restricted Interfaces and effect mappings,
  including missing A2A payload facts and MCP principal rebinding requirements;
  it adds no verifier, durable task backend or production qualification.

### Fixed

- Redact Debug/pretty Debug for the four unreleased MCP HTTP response types:
  notifications, complete Tool results, remote errors and the outer call response.
  Retain fixed kind/count/presence fields and the allowed error code; the outer
  response never recursively formats continuation Tool metadata. Raw getters,
  existing Clone/PartialEq, wire, MRTR resume, resource limits and effect/retry
  mappings remain unchanged. Bare InputRequired values keep their existing
  Debug scope. Fixed source passes six local gates (2,099 workspace tests,
  6 original ignored, 48 doctests) and the eight real-TCP JSON/SSE branches;
  see the [bounded record](docs/qualification/experimental-mcp-http-diagnostics-2026-10-16.md).
  Prior canary/lint failures stay recorded. Separately, this tree passes both
  complete targeted Native suites (6 and 10 tests) on each PostgreSQL 16.15/17.11,
  using freshly built default-feature binaries, serial/default-stack opt0 with
  assertions and overflow checks. This does not transfer older PostgreSQL
  evidence or qualify full Store/Runtime Native, full CI, deployment, stdio or R1.

- Confirm test-owned container and volume removal before PITR drill success markers, retaining original budgets and best-effort failure cleanup. Subsequent composed-main original-opt0 local gates pass; see the [bounded record](docs/qualification/experimental-composed-request-facts-pitr-2026-10-15.md).

- Recover experimental original absolute Graph Timer Waits after their due
  instant without reexecuting successful nodes or inventing registrations or
  firings. Schema 36 binds the complete original condition inventory and
  `GraphWaitEffect::{Waiting, Ready}` to the whole Root/scoped transaction.
  Root lifecycle/Agent Loop exposes `ReadyReleased` with its original receipt
  and fence-release result; scoped Ready retains fresh planning and the shared
  quantum. Existing wire formats and complete owning/post-guard authentication
  remain intact. Final CI, full fault/compatibility and production acceptance
  remain open under Draft RFC-0022.

- Reuse a complete ordinary scoped base authentication within one experimental
  public attempt read, retaining all physical completion, result and binding
  checks. Mutations and post-guards keep the original full path. Erase the
  complete private Start verifier Future to resolve full-workspace Runtime
  trait recursion without raising compiler or resource limits. All static
  gates and the four original local Native suites pass on each PostgreSQL
  version: Store 252, Runtime 185, integrations 2 and example 1. Original
  higher-fence, actual seven-level, serial and shared-quantum regressions pass
  with default resources. Conditional worker/resource entries are distinguished
  from actual scenarios; forced local backup/restore and same-tree Linux CI
  PITR pass separately. Linux deep recovery, remaining CI/composition, capacity
  and production qualification remain open. Same-head CI has 11 successful
  jobs and two ordinary PostgreSQL jobs cancelled at the original 30-minute
  limit: Store passes 252/252; Runtime has no terminal summary.

- Keep complete experimental Driver startup, iteration, barrier and node phases,
  Root admissions and checkpoints behind private heap boundaries. The original
  two-level higher-fence and SQL-role regressions now pass on PostgreSQL 16/17
  with default stacks and unchanged owning verification and resource limits.
  That earlier layout-only candidate's seven-level recovery failed with lease
  expiry; its evidence remains separate from the later base-proof candidate.
  Full qualification remains open under Draft RFC-0022.

- Retain experimental Driver recovery and node-execution mutation
  acknowledgements across guardian failure, completion and cancellation.
  Cooperative stop keeps committed quarantine, Start and renewal ACKs alive;
  the original execution deadline stops application work while the Driver
  drains an in-flight renewal. Full owning/live-fence checks remain required
  before dispatch and result commits. Final combined qualification remains open.

- Preserve an experimental Driver's original acknowledged Join, Wait, retry
  or ownership-release outcome when its guardian subsequently fails. External
  shutdown is distinguished from guardian cleanup, so guardian-only cancellation
  still fails closed. Real executor calls select execution-renewal fault cuts;
  the original lease durations and complete owning checks remain unchanged.

- Bound experimental scoped DIRECT observation by the original admitted
  deadline and node timeout, forwarding AgentLoop shutdown without treating
  incomplete costs as zero. Scoped cancellation retains the original fence
  while observing, then waits for complete validation/quarantine, renewal and
  Closure acknowledgements. Guardian failure cannot discard a committed
  mutation ACK. Cleanup may outlive admission without granting launch.

- Keep complete child admission intents and ownership records on the heap during
  Join and account recovery. Original role-profile recovery now fits the default
  thread stack while retaining canonical bytes, complete ownership/terminal
  checks, lock ordering and the existing public record interfaces.

- Erase complete private MCP success and known-error reconciliation Futures at
  concrete heap boundaries for strict pinned-nightly compilation, preserving
  authorization, complete durable evidence, non-retryable effects, cancellation
  and timeout.

- Stop allocating MCP activation/request and A2A request IDs at their original
  exclusive ceilings without overflow or ID reuse. Use an MSRV-compatible
  checked compare-and-exchange loop in place of the deprecated atomic update API,
  preserving each allocator's original memory ordering and wire range.

- Attribute reused priced model/tool terminal evidence to its original physical
  attempt, retaining exact bindings and cumulative lifecycle charges. Later
  attempts do not charge that I/O again; unknown outcomes, reconciliation after
  the current Start and unpriced evidence preserve their original usage rules.

- Flush deferred Join-registration guards before rechecking the released
  original worker epoch, lease expiry and Agent deadline. Expiry rolls back
  registration, journal advancement and lease release together.

- Flush deferred child-admission guards before the final original-worker,
  parent/child deadline and reservation checks. A lease expiring during those
  guards now rolls back fresh child admission and its complete parent/account
  projections. Existing logical-key ACK recovery still precedes fresh checks.

- Replay schema-34 complete Root graph dispositions with the deployed schemas
  and reducer, binding the original lifecycle, projection, source journal and
  full Wait facts across all three reads. Canonical Wait transcript rendering
  is independent of connection time-zone, date-style and bytea output settings. Legacy
  unprovable dispositions remain operational errors without quarantine.
  A bounded Root-prefix proof is additive during qualification; all original
  owning verifiers remain and total SQL complexity is still unqualified.

- Decode `JournalPayload.data` and `AgentResult.output` with their constructors'
  existing maximum JSON profile. Constructor-accepted large values now survive
  recovery; complete journal envelopes, streaming bounds, result provenance and
  accounting remain validated. The default standalone JSON profile is unchanged.

- Freeze framework call sites without requiring an application node executor.
  Conflicting caller executors and missing ordinary executors remain errors;
  exact target pins, serial sibling calls and seven-level limits are preserved.

- Erase complete private claimed-read, executor and delayed-retry Future types
  at concrete heap boundaries. Strict pinned-nightly compilation retains full
  owning proofs and the original execution, cancellation and deadline behavior.

- Independently rederive Root delayed-retry decisions from complete actual
  result and physical-attempt history under the Run lock, sharing the scoped
  validator. Public pure-planner metadata can no longer fabricate a completed
  sibling or replace a stored SafeAfter boundary to starve scheduler claims.
  Released-gate ACK remains a read-only observation without launch authority or
  proof of the erased original worker attempt and complete original plan.

- Recheck the original live worker and database clock after delayed-retry Run
  updates. A slow trigger past lease expiry rolls back; an inclusive due retry
  restores the original lease, Root/leaf heads, journal and queue age before Due.

- Type-erase the private claimed history observer, child reconciliation retry,
  provider model execution and read-only tool wave futures. This removes the
  pinned nightly Send autosolver overflow without changing public signatures,
  retry/cancellation policy, durable effect ordering or compiler recursion limits.

- Reject unexpected result namespaces at an exact checkpoint in the same paging
  snapshot. Root and scoped reads report durable corruption; claimed recovery
  uses the original live worker context to quarantine the Run.

- Keep the immutable whole-return Wire intent on the heap during authenticated
  scoped result paging. This preserves canonical JSON, digests and all proof
  checks while avoiding a Serde visitor stack overflow after multiple returns.

- Keep complete return/closure caller-anchor verification and ordinary node
  completion restoration behind heap-backed Future boundaries. This prevents
  closure completion witnesses from enlarging every ancestor replay Future
  while retaining double-completion rejection and full seven-level recovery
  checks on the default Linux thread stack.

- Reject a Root checkpoint relabeled as a scoped model/tool activation before
  frame lookup, preserving existing invalid-activation errors for new requests
  and treating the same alias in stored invocation facts as corruption.

- Keep complete scoped wait authentication in a separate heap-backed Future,
  preserving whole terminal checks without inflating non-wait barrier replay
  or overflowing the default thread stack in seven-level return recovery.

- Require actual libFuzzer execution and RSS statistics before marking the
  bounded ASan mutation profile complete; report the actual count and whether
  the execution cap was reached, preserving the original execution/time ceilings.


- Retain stable request identities while retrying conservatively classified
  PostgreSQL transaction contention in the 24-way whole-frame return test;
  production lock deadlines and once-only settlement assertions are preserved.


- Execute source-schema downgrade fixtures as complete SQL batches, preserving
  dollar-quoted PostgreSQL function bodies introduced by whole frame returns.
  Store and Runtime upgrade regressions retain their actual nonempty facts and
  exact migration checksums.

- Draft RFC-0023 corrects `MediaType` input/default schemas to accept the
  case-insensitive names already supported and normalized by the reader.
  Nested input schema pins explicitly change; all output pins and canonical
  wires stay exact. Immutable registries require new input schema/Tool versions
  and matching retained executables. The real CI reproducer remains a permanent
  regression. Source `41bf0b8c` passed all fourteen CI/dependency checks and
  local/CI bounded ASan; RFC acceptance remains pending.

### Added

- Add the experimental active-frame delayed retry gate with whole current scope
  proof and complete bounded physical-history replay under the Run lock. Reuse
  the existing Root outcome/schema and original Core retry policy. Released-gate
  ACK observes only epoch/head/journal/gate and grants no launch or proof of the
  erased original worker attempt or complete original plan. Native PostgreSQL
  seams cover concurrent ACK, exact corruption isolation, slow update clocks,
  64 physical attempts, deadline cleanup and seven-level noninitial leaves.

- Add experimental claimed Root/lifetime-frame application replay against exact
  deployed graph schemas/reducers, complete scoped successors/dispositions and
  returns, transaction-free callbacks, bounded direct-edge transcripts and
  original journal/lifecycle/scheduler/wait/head/database-time fence revalidation.
  A Copy Root report is measured in the same pass, without repeating callbacks.
  Unprovable legacy Root
  dispositions fail operationally without quarantine or a report. Nested Driver
  integration and RFC-0022/R1 qualification remain required; see the bilingual
  scoped graph application replay contract.

- Add experimental claimed active-frame ready planning and bounded scoped result
  pages. Whole leaf/ancestor proofs and exact namespace/head/journal observations
  reuse completed work and complete physical history, then revalidate the current
  leaf and database-time fence. Root reads keep their existing interface; actual
  application replay and dedicated durable scoped starts remain required.

- Add experimental whole-stack closure after an original cancellation or sealed
  failure. Schema 33 atomically preserves every physical caller's old fence in
  separate immutable control-plane completion facts, freezes complete priced
  DIRECT usage, clears the stack/lease and retains the exact Root checkpoint.
  Whole reads and lost acknowledgements authenticate the original decision;
  terminal Run writes cannot bypass open frames or replace frozen usage.
  Scoped Tool/Model mutations and history now authenticate their actual frame
  activation and head, with a final live-fence check after deferred guards.
  Unknown effects, in-flight ordinary/provider work and unsettled children
  block cleanup. Exact catalog and the 52-table trusted-role profile advance
  together. Nested Driver dispatch, the full fault matrix, immutable-source
  qualification and RFC-0022 acceptance remain required.

- Add an experimental, bounded Store snapshot of the complete active frame
  ancestry, current leaf and physical caller; authenticate returned history and
  whole wait terminal facts in the same transaction before Driver recovery.
  Structural DIRECT floors grant no dispatch authority or complete accounting.

- Experimental whole scoped waits atomically bind the leaf successor, complete
  registrations and original lifecycle revision while releasing the Run lease.
  Recovery authenticates complete resolution/firing/abandonment facts and resumes
  the saved scope under a new fence. Schema 32 guards preserve exact prior
  non-wait bytes, the 50-table ACL and fail-stop quarantine. Final qualification,
  all-frame closure and nested Driver dispatch remain RFC-0022 gates.


- Experimental whole frame returns authenticate an existing terminal proof and
  atomically settle the current framework caller, exact parent result, stack
  pop and reserved journal fact. Recovery precedes fresh callbacks/authority;
  newer fences require an actual rebound caller. Shared DIRECT floors survive
  returns and later mutations. Transaction-owned compact replay proofs avoid
  repeated nested history traversal; default-stack seven-level cascades, races,
  component rollback, expired leases, corruption and nonempty source upgrades
  have native regressions. Schema 31 pins exact guards and 50-table ACLs.
  Scoped waits, closure, actual nested driver and RFC-0022 acceptance remain
  pending.

- Add experimental atomic framework caller rebinding for RFC-0022. A newer
  worker records a bounded physical attempt and whole immutable binding to the
  existing child, preserving its activation/checkpoint and shared Run budget.
  Schema 30 pins exact caller guards and runtime ACLs; ordinary framework
  completion remains rejected pending whole return. RFC-0022 remains Draft.

- Experimental scoped Store barriers atomically bind the exact complete result
  set and consumptions, worker event, isolated successor, immutable witness and
  active frame head. Planning uses actual pinned schema/reducer dependencies
  outside the mutation transaction; locked commit repeats scope, fence, shared
  usage and settled child accounting. Whole historical recovery authenticates
  bounded forward lineage; acknowledgment loss grants no new launch. Schema 29
  adds exact deferred guards and a 48-table trusted-server ACL profile. Scoped
  continuation/terminal barriers are prerequisites for framework return;
  scoped waits, rebinding, all-frame closure and actual driver remain pending.

- Experimental Store transactions durably start ordinary nodes in an
  authenticated active initial frame and commit their success/result or failure
  atomically. Scoped historical reads verify the whole entry and checkpoint;
  bounded attempt histories, same-fence in-flight recovery and takeover rules
  are reused. SQL guards prevent suspended-scope completion or independent
  framework-caller completion. Deferred guards execute before the final live
  lease/deadline check. The stack CHECK catalog stays exact after logical
  dump/restore. Framework rebinding/return, scoped waits, closure and
  actual nested driver dispatch remain pending in Draft RFC-0022.

- Experimental Rust-only compound frame-entry planning binds the complete event,
  framework start and isolated checkpoint with one domain-separated projection.
  Its versioned local event schema carries identity/digest data without copied
  state. Ordinary append/checkpoint/node-start APIs reject the reserved compound
  kind without partial facts. Migration 28 adds an experimental atomic Store
  admission under the original Root admission, inherited ceilings, shared usage,
  exact journal head and live fence. Authenticated reload precedes fresh schema
  callbacks or lease checks; SQL guards reject incomplete scoped checkpoints,
  substituted heads and legacy waits during a child. Scoped execution, return,
  waits, process-loss qualification and nested dispatch remain pending in RFC-0022.

- Draft RFC-0022 for same-Run namespaced graph frames, with five executable
  private identity checks. It specifies isolated state, bounded scope, durable
  parent continuation and compound journal bindings; runtime/persistence
  implementation and acceptance remain pending. No execution support is added
  by the Draft or constructor prototype.

- Experimental Core frame identity, scoped checkpoint and compact head data for
  RFC-0022. Strict readers reconstruct scope/checksums and reject owner/schema,
  ancestry, tenant/Run/graph, journal-order and predecessor substitution. Scoped
  ready activations use the existing digest domain; root wires and schema pins
  remain exact. Three new reader/schema entries, current-source vectors and
  independent checksum/chain models extend the inventory. Compiler, runtime and
  SQL integration remain pending; these data types do not enable nested execution.

- Experimental same-Run call declarations bind exact targets, return routes and
  finite depth/frame-start limits into compiled graph definitions. Compiler and
  startup closure checks reject incompatible schemas/owners, parallel callers,
  stale or missing target pins, excessive closure depth/bytes and application
  executors installed at framework call sites. Static composition cannot drop
  isolated frame declarations. The `CompiledGraph` and `ChildRunAdmissionIntent`
  input/output schema pins explicitly change; old root wires and definition pins
  remain exact. Transactional runtime/SQL execution and acceptance remain pending.

- Experimental scoped recovery and frame barrier planning preserve frame identity,
  exact ready activations, canonical bounded result order and local lineage.
  Existing attempt-history/fence classification and pinned schema/reducer/control
  validation are reused. The legacy root barrier remains root-only. A separate
  frame intent wire/schema, current-source constructors and independent checksum
  model cover the new data boundary; transactional execution remains pending.

- Experimental graph call entry/return preparation verifies exact declared
  callers, target pins and checkpoint positions, copies isolated state, and
  converts only a matching terminal child plan into the fixed-route parent
  update. Existing data/wire/schema types are reused; no persistence or dispatch
  authority is inferred from preparation.

- PostgreSQL migration 27 scopes checkpoint positions and parent/activation/
  ownership/Join references, fixes root pointers with generated empty namespaces,
  and makes root queries/decoding explicit. Exact installed catalog checks cover
  columns, constraints and indexes. Nonempty source-schema 26→27 and typed scope
  FK checks run on PostgreSQL 16/17; compound frame transactions and nested
  execution remain Draft. The trusted-server role profile now requires schema 28 and its exact 47-table allowlist.

- Independent graph state, route and checkpoint property models: node insertion
  and result input order preserve fixed committed facts, while state/route
  references and canonical checksum preimages verify the actual root barrier.
  Unicode checkpoint chains retain exact lineage and reject altered state.
  Product APIs, wire/schema pins, fixtures and dependencies remain unchanged.

- Seven independent composite budget property models covering all usage fields,
  topology peaks, finite partial-layer intersections, currency allowlists,
  narrowing, reservation capacity and cumulative deduction. Wide arithmetic
  provides the reference; per-field overflow and currency-count controls retain
  all production API, wire, pin and dependency contracts.

- A closed canonical matrix for all 71 serialized public Core enums and their
  298 alternatives, including 28 newly constructed branches. Typed schema-case
  completeness, wire/digest stability, raw duplicate/unknown-field and array
  rejection run alongside both fuzz schema oracles. The 43 previous fixture
  documents and all existing input/output pins remain exact.

- Independent nested JSON resource and canonical-tree property models, with
  exact boundaries, one-unit tightenings, random narrowed profiles and extension
  restriction of previously wider values. Fixed-seed and ordinary random runs
  preserve all production limits, wire/schema pins and existing properties.

- Isolated, pinned ASan/libFuzzer qualification for strict bounded JSON/JCS,
  all 307 public Core readers and the actual offline runtime schema registry.
  Every seed is replayed before finite mutation; source/lock integrity, owned
  process cleanup and retained synthetic reproducers form a dedicated CI gate.
  A separate closed inventory pins all 308 serialization schemas.

- A compiler-checked closed Core root-export inventory covering 555 types,
  307 typed reader/writer wires and 308 generated schema pins. Two output-only
  types retain their producer boundaries; 246 reviewed Rust-only instantiations
  reject accidental serialization. Complete admission, child accounting/Join
  and model continuation wires are reproduced by their existing constructors.

- Complete execution wire fixtures and typed canonical readers for 67 Core
  types, with closed-object, duplicate-key, checked-digest and identity-collection
  rejection evidence. Eight existing constructor suites retain their earlier
  digest expectations and now compare complete wires with the frozen document.

- A synchronized 100-appender PostgreSQL journal qualification profile that
  verifies a concurrent lifecycle transition, exact event/intent identities,
  contiguous digest-linked history, projected head and lost-ack retries during
  and after contention. Retry count, joined execution and task ownership are
  bounded; substituted lifecycle projections fail without changing history.

- Typed positive/negative canonical fixtures for 38 Core value types, including
  every generated UUIDv7 identity and bounded tenant, scheduler and submission
  identifiers. Thirty-one bounded property models verify constructor/Serde
  agreement, precision, checked arithmetic, UTF-16 canonical ordering, scope
  narrowing and exact extension limits, with a reproducible CI seed.

- An explicit `register_rust_output_type` startup helper for pinned Serde
  serialization schemas, plus compile-fail Tool schema and credential guards.

- Compile-time regression evidence that execution contexts, cancellation
  handles and Agent HTTP credentials cannot enter Serde durable records.
  This preserves existing boundaries without changing the public API.

- A local RFC 9068 RS256 Agent HTTP authenticator backed by AWS-LC, bounded
  operator-provisioned JWKS, atomic generation-checked key rotation, expiring
  shared tenant policy and separate resource authorization. PostgreSQL 16/17
  HTTP/SSE and real TLS Keycloak qualification cover its declared boundaries;
  trusted key delivery and refresh remain host deployment responsibilities.

- A provider-native invocation budget source for sequential Tool graphs. It
  verifies the admitted plan and reconstructs spent model/tool capacity from
  durable PostgreSQL evidence before external dispatch; parallel Tool graphs
  are rejected until atomic capacity reservation is available.

### Changed

- Configure only the PostgreSQL CI test job for basic optimization while retaining
  debug assertions, overflow checks, original fixtures and the 30-minute budget.
  Record the bounded local experiment: six gates and both PostgreSQL versions'
  original four Native suites plus separate authorization facts exit zero, while
  the runner's final missing-cache-tag guard remains a preserved failure. The
  original full CI default-feature profile remains pending; no production
  capacity or Supported claim follows. See the [execution record](docs/qualification/experimental-postgres-test-profile-2026-10-15.md).

- `InProcessAgent::run` now performs its post-admission and polling reads through
  the caller-retained submission key, with an exact Run-ID consistency check.
  Deployments using this convenience path must grant `Read` on that submission
  key; a Run-ID-only read grant no longer suffices. This avoids requiring a
  tenant-wide read grant for a one-request typed Agent run.

### Fixed

- Core object and tagged-object readers require maps instead of accepting
  positional struct/enum sequences. A streaming guard reuses existing field,
  constructor and integrity checks; scalar/collection and opaque JSON shapes
  stay unchanged. Valid wires and all input/output schema pins remain exact;
  sequence-based binary decoding is outside the canonical JSON contract.

- Canonical timestamp parsing validates decimal characters before arithmetic,
  returning the existing format error across overflow-checking profiles.
  Exhaustive ASCII-position and fixed-length Unicode regressions cover direct
  and nested Serde readers, with the synthetic fuzz failure retained as a seed.
  Valid timestamp bytes, ranges and schema pins remain unchanged.

- `Failure`, `ToolError` and Capability lifecycle output schemas now use their
  actual borrowed serializer wires, preserving optional omission behavior.
  Existing input schema and wire pins stay exact. Incompatible frozen output
  pins fail before dispatch and require new schema/Tool versions; actual parent
  schema documents retain regression evidence without rewriting admitted pins.

- Empty internally tagged Core variants now reject extra fields instead of
  silently discarding them in Serde. This closes node control/state, prepared
  invocation, journal source/expectation and pending-Run readers while preserving
  valid wire bytes, public Rust variants and generated JSON Schema pins.

- Typed Tool adapters now generate input schemas for deserialization and output
  schemas for serialization, explicitly using JSON Schema 2020-12. Directional
  field names, skipped fields and optional output therefore participate in the
  correct startup pin. Incompatible existing output pins fail before dispatch
  and require new schema/Tool versions; admitted pins are never rewritten.

## [0.1.0-alpha.1] - 2026-09-21

This is the first public, lockstep crates.io preview. Breaking changes may occur
between alpha identifiers; consumers must pin the exact version. The supported
MSRV is Rust 1.88.0. See the
[versioning and release policy](docs/versioning-and-releases.md).

### Fixed

- Skill activation evidence now fails closed while decoding: direct activation
  sources reject an injected parent-window field, and acting-window durations
  enforce the documented one-second through 24-hour bound during both Serde
  deserialization and JSON Schema validation.

- PostgreSQL transaction startup now completes and rolls back after caller
  cancellation, preventing a server-side transaction from returning to the pool
  before `SQLx` records its transaction depth. Read and mutation transactions
  also select their isolation and access modes in the initial `BEGIN` command.

### Changed

- Upgraded the official Rust MCP SDK from `rmcp 3.3.0` to `3.4.0` and migrated
  every client/server handler to the non-deprecated `ClientConfig` and
  `ServerConfig` contracts. The frozen MCP 2026-07-28 client gate remains green
  for all 32 scored scenarios; corrected protected-resource metadata probing in
  the SDK removes two redundant successes, so the refreshed evidence records
  371 scored assertions with zero failures instead of 373.

- Upgraded `object_store` to 0.14.2 and refreshed the website toolchain to Astro
  7.3.3, Prettier 3.9.7 and `@types/node` 24.13.5. Locked dependency audits,
  cross-platform Rust tests and the complete website verification suite remain
  mandatory merge gates.

### Added

- A lockstep public Alpha release boundary for seven product crates, exact
  prerelease dependency pins, an explicit Semantic Versioning/MSRV/durable-data
  policy, bilingual adoption documentation, and resumable release automation.
  The protected OIDC publisher packages in dependency order, byte-compares
  registry downloads, and compiles a registry-only external consumer.

- A typed, HTTP-free `InProcessAgentRuntime` convenience path that owns and
  fail-stop supervises the real durable Worker and maintenance roles. It keeps
  PostgreSQL admission, authorization, caller-retained idempotency keys,
  bounded polling, terminal provenance/accounting validation, explicit joined
  shutdown, and same-key recovery after timeout or process replacement. A
  compiled example and bilingual production migration guide are included.

- Production `ToolSchemaRegistry` support on the offline JSON Schema registry,
  typed Rust schema generation/pinning, and a compiled local Tool registration
  example. Bilingual guides now cover local/MCP/A2A provider coexistence and a
  production Skill-composition model built from immutable capabilities,
  shared-state subgraphs, durable child runs and guarded MCP Skills.

- Production-safe PostgreSQL startup configuration with a URL-redacting
  `PostgresStoreConfig` builder, closed environment-variable parsing, distinct
  production migration/runtime credentials, explicit bounded development
  defaults, optional auto-migration through a short-lived migration pool, and
  bilingual deployment guidance. Existing explicit migration and connection
  APIs remain available.

- Durable MCP Skill activation approvals and acting windows. Activation now
  binds exact tenant/run/thread scope, host origin, URI, complete Manifest,
  direct/nested source and version-pinned policy evidence to caller-retained
  idempotency IDs and a bounded lifetime. PostgreSQL schema 26 commits immutable
  approval/window rows at database time, supports exact restart resume, records
  immutable idempotent revocation, validates nested parent authority, and
  serializes fresh per-operation receipt commits against revocation before
  provider I/O. A receipt committed first remains authorized and idempotent;
  revocation ordered first blocks a new receipt. Remote Skill bytes remain
  process-local and are reverified.

- Durable per-operation MCP Skill Tool authorization receipts. Policy now
  returns version-pinned grant identity plus policy/decision digests;
  `McpSkillBoundTool` requires a durable sink and commits payload-redacted
  canonical evidence before execution or reconciliation provider I/O. PostgreSQL
  schema 25 binds immutable receipts to exact tenant/run/thread/invocation/
  attempt/origin-event evidence, supports exact idempotent recovery and bounded
  verified audit pages, and rejects mutation. Unavailable storage yields safe
  pre-dispatch delayed retry evidence; crossed or rejected evidence fails closed.
  Receipts prove authorization, not provider dispatch or external effects.

- MCP Skills Tool-runtime binding through `McpSkillBoundTool`. The adapter
  freezes an exact owner/name/version and domain-separated complete descriptor
  digest, requires explicit Host-code exposure classification, requests fresh
  policy approval separately for execution and reconciliation with exact
  durable correlation and bounded schema-bound input, consumes the
  acting-window permit immediately before provider I/O, maps denial to
  pre-dispatch effect evidence, and registers in the existing immutable Tool
  registry so the durable attempt ledger remains authoritative.

- Final SEP-2640 MCP Skills static Client and Host Profile with explicit
  capability opt-in, complete bounded manifest/frontmatter validation,
  host-assigned origin identity, approval before lazy verified reads, exact
  URI/size/SHA-256 reconciliation, immutable process-local cache isolation,
  acting-window entry retention, fresh nested-Skill consent, and non-cloneable
  lifetime-bound per-call execution permits. Loopback adversarial tests cover
  denial-before-read, digest and frontmatter drift, cache behavior, metadata,
  nested activation and ordinary-client non-advertisement. Dynamic manifests,
  disk materialization, signatures and automatic
  discovery-to-Agent composition remain explicitly unclaimed.

- Final SEP-2640 MCP Skills static Server Profile with explicit
  `io.modelcontextprotocol/skills` negotiation, immutable startup catalogs,
  bounded duplicate-free Agent Skills frontmatter, complete exact-byte
  SHA-256/size manifests, paginated `skills/list`, direct `skills/get`,
  manifest-bound `resources/read`, scope and dynamic authorization filtering,
  private cursor binding, URI/path collision refusal, and end-to-end HTTP
  attack-surface tests. This remains a separate pre-alpha server claim.

- Closed, versioned catalog for all 38 committed `stateknot-core`
  compatibility fixture documents. The gate strictly parses bounded JSON,
  preserves deliberately non-canonical negative vectors through exact content
  SHA-256 digests, binds the ordered metadata with a domain-separated RFC 8785
  root, rejects inventory/path/schema drift, and requires an executable Rust
  compatibility-test reference for every entry. Complete Tool authorization
  receipts and Skill approval/open/window/revocation records now freeze full
  wire forms, retained and canonical-wire digests, payload redaction, closed
  schemas, and fail-closed tamper vectors. This advances RFC-0001 validation
  item 2; exhaustive type-level fixture coverage remains open.

- Four executable `stateknot-core` public contract examples for the first Agent,
  typed Tool registration, provider-neutral Model stream validation and explicit
  protocol mapping. A locked Cargo-metadata test fails on every unreviewed direct
  core dependency change, and CI compiles all four examples explicitly on MSRV.
  Bilingual guides record the exact no-I/O boundary. This closes only RFC-0001
  validation item 1; RFC-0001 remains Draft and no API-stability claim is added.

- Public-alpha `stateknot-testkit` host qualification harness with recorder-owned
  monotonic phases, bounded HDR latency distributions, checked operation/safety
  counters, stable fault cases, deterministic integer objectives and canonical
  SHA-256 integrity envelopes. A reduced real Agent host profile on PostgreSQL
  16/17 covers HTTP-to-terminal execution, SSE delivery/reconnect, dependency
  readiness loss/recovery, protected operations and rolling replacement. Reduced
  results are always non-release evidence and may explicitly report unmeasured
  release-only saturation/fairness; no production SLO, provenance signature,
  runtime deployment, durable schema or business protocol changes.

- Independently owned read-only host operations with explicit InspectHost
  permission, opt-in introspection scope and a separate bounded expiring operator
  ACL; sanitized lifecycle/counters, request/transport ceilings and joined drain.
  PostgreSQL 16/17 and dedicated real TLS identity qualification, RFC-0016 and
  bilingual guides. No schema/dependency change, administrative writes or runtime
  deployment; downstream exhaustive operation matches must handle the new variant.

- Concrete co-located Agent host owning actual HTTP, Worker and maintenance
  bindings: exclusive ingress ownership, ordered startup, sibling readiness
  gates, fail-stop supervision and HTTP → Worker → maintenance joined drain.
  Includes PostgreSQL 16/17 lifecycle and real TLS identity/resource-policy
  composition qualification, RFC-0015 and bilingual guides. No new dependency,
  migration, public health route or stable production release claim.

- Owned Agent maintenance over concrete deadline, child cancellation/settlement,
  Join publication and failure-close services: explicit bounded tenant rotation,
  retained cursors past item errors, actual store plus mandatory host readiness,
  fail-stop deadlines and joined shutdown. Includes PostgreSQL 16/17 durable
  effects, failed-page and SIGKILL recovery qualification, RFC-0014 and bilingual
  guides. No migration, dependency change, automatic retention or stable release claim.

- Owned concrete tenant/fair scheduling Worker with fixed concurrency, actual
  schema/registry/policy and mandatory host readiness, finite pacing and deadlines,
  fail-stop drain and cancellation-safe joining of nested Graph node futures.
  Includes PostgreSQL 16/17 fault and fresh-process recovery qualification,
  RFC-0013 and bilingual operations guides. No migration or dependency-version
  change, implicit maintenance jobs, public health endpoint or stable release claim.

- Concrete offline default-deny Agent resource authorization with digest-pinned
  artifacts, exact selectors, explicit tenant operators, restrictive budgets,
  deterministic retained admission evidence, bounded freshness and atomic CAS
  replacement. Unrelated ACL refresh preserves lost-response submission recovery.
  Includes PostgreSQL 16/17 HTTP/SSE and real Keycloak composition qualification,
  RFC-0012 and bilingual guides. No migration, dependency change, automatic
  ownership, durable read-decision ledger or stable production-release claim.

- Concrete bounded OAuth token introspection for Agent HTTP: verified HTTPS,
  exact access-token claim/scope checks, secret rotation, default-deny expiring
  tenant bindings with atomic CAS replacement, and real negative-canary readiness.
  The existing resource authorizer remains mandatory. Includes pinned Keycloak
  TLS/rotation/revocation/SSE qualification on PostgreSQL 16/17, RFC-0011 and
  bilingual operations guides; no JWT/JWKS, schema change or stable release claim.

- Owned loopback HTTP ingress runtime with mandatory actual store/schema,
  executable registry and host dependency readiness; single-flight probes,
  freshness-gated admission, explicit connection/header/lifetime ceilings,
  cancellation-safe joining and bounded graceful/forced drain. Includes real
  PostgreSQL 16/17 lifecycle tests, RFC-0010 and bilingual operations guidance.
  No migration, default verifier, inline Worker or stable release claim.

- Opt-in resumable Agent activity SSE with exact PostgreSQL journal cursors,
  payload-free notifications, independently verified current snapshots (including
  quarantine-only changes), repeated credential/resource authorization, separate
  stream capacity, finite lifetime/queue/output limits and drop/shutdown cleanup.
  Real HTTP/PostgreSQL 16/17 tests include fresh-process suffix recovery and
  unpolled response backpressure. Includes RFC-0009 and bilingual tutorials.
  No migration; no token streaming, historical
  snapshot reconstruction or stable production release claim.

- Updated locked Rustls from 0.23.43 to 0.23.45 for
  [RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285.html),
  which concerns TLS 1.3 handshake encryption-level validation. The dependency
  gate remains mandatory; no advisory exception was added.

- Authenticated Agent HTTP v1 JSON ingress over `AgentServiceV1`: durable
  submit/read/key lookup, caller-retained two-phase cancellation, mandatory
  credential and resource policy, exact Host/Origin rules, bounded duplicate-free
  JSON, finite response/concurrency/deadline limits and cooperative shutdown.
  PostgreSQL 16/17 HTTP qualification covers 24-way admission, lost submit/cancel
  responses, authorization-before-storage, hostile input and overload. Includes
  RFC-0008 and bilingual website tutorials. Existing migrations and dependency
  versions are unchanged; no anonymous verifier, inline execution or stable
  production release is claimed.

- Separately scoped known-effect MCP Tool error reconciliation: mandatory current
  resource/evidence authorization, host-owned non-retryable failure provenance,
  atomic Failed/audit persistence and exact lost-receipt recovery. Closed input
  rejects uncertain/partial effects, retry authority and private error details.
  PostgreSQL 16/17 qualification covers both effects, 24-way error and mixed
  success/error races, revocation, startup schema drift and legacy v1 schema
  compatibility. Includes RFC-0007, bilingual operations guides and website
  guidance. No migration, dependency or protocol-version changes; not provider
  settlement, automatic retry or a production-release acceptance.

- Official website footer now displays the operator-confirmed ICP website filing
  `冀ICP备2026036754号-1` with the MIIT lookup link across both languages, including
  documentation and error pages; browser tests cover exact text, safe new-tab
  attributes, route parity and responsive layouts.

- Authenticated inline MCP Tool result reconciliation with mandatory pre-lookup
  subject/resource/evidence policy, closed request/receipt schemas, host-only
  lease fencing, atomic authorization audit, indexed verified exact revisions
  and lost-response receipt recovery. Real HTTP/PostgreSQL 16/17 qualification
  covers tenant/attempt/result refusal, active leases, 24-way first/duplicate
  submissions and fresh-service recovery. Includes RFC-0006 and bilingual
  operations guidance. No migration or dependency version changes; this does
  not expose general Worker execution or artifact reconciliation; known errors
  use the separate profile above.

- Executable PostgreSQL 16/17 schema-26 trusted-server role profile separating
  non-superuser migration ownership, column-scoped runtime writes and dedicated
  fairness-reservation retention. Includes atomic apply/read-only effective ACL
  audit, drift/default/membership/forbidden-SQL checks, actual role-separated
  Agent/Join/provider recovery, 24-way submission/completion races and CI evidence.
  Removes redundant immutable node-attempt/submission row locks while retaining
  Run/advisory serialization, so append-only grants actually work. Bilingual
  deployment/rotation/incident guidance defines the trusted-account boundary;
  untrusted-worker SQL/service isolation and full failure-matrix qualification
  remain gated. Schema 25 added the immutable Tool authorization receipt ledger;
  no dependency or lockfile change is required.

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
- New pre-publication `stateknot-runtime` crate with immutable, digest-pinned,
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
- Initial pre-publication `stateknot-store-postgres` slice for PostgreSQL 16/17,
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

- Chinese website and repository guides now distinguish durable execution
  (`持久执行`), recovery capability (`可恢复`), persistence (`持久化`) and storage
  durability (`持久性`) instead of the literal `耐久`. Includes natural page
  headings, consistent navigation/search/metadata, a concept explanation and
  browser regressions. Rust APIs, example code, URLs and capability gates are
  unchanged.

- PostgreSQL CI now serializes top-level tests that intentionally share one
  migrated schema, while preserving each scenario's internal multi-threaded
  concurrency pressure and eliminating unrelated Tokio-runtime starvation.
- Concurrent identical graph registrations now converge through every unique
  index arbiter before verifying the immutable stored definition, instead of
  leaking a PostgreSQL `23505` race from the redundant exact-reference index.
