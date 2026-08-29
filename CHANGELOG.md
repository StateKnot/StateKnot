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
- Protocol-neutral tool descriptors with digest-pinned schemas, closed and
  cross-validated side-effect/idempotency semantics, non-granting resource
  requirements, bounded cancellation/progress behavior, finite input/output/
  artifact/concurrency/time ceilings, redacted diagnostics, closed schemas,
  property tests, and versioned wire fixtures.
- CI, dependency policy, issue forms, and security reporting guidance.
- Crate package metadata and file lists that retain the Apache-2.0 SPDX
  expression while embedding `LICENSE`, `NOTICE`, and `README.md` in every
  distributable source archive.
