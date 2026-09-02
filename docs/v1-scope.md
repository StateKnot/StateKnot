<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot v1 scope baseline

> Status: M0 baseline<br>
> Baseline date: 2026-08-28<br>
> Change control: a scope change that affects a public API, durable record,
> protocol claim, security boundary, or release guarantee requires an RFC.

This document fixes the product boundary used to design and qualify StateKnot
v1. It does not make the pre-alpha repository production-ready. A capability is
supported only after its implementation, tests, documentation, compatibility
evidence, and release gates have all shipped.

Current implementation note: the pre-alpha repository now contains
`AgentServiceV1`, an authorization-first embedding facade, and
`McpRemoteTool`, one strict MCP `2026-07-28` stateless client-side Tool profile.
They are implementation slices toward the surface below, not claims that the
stable network Agent API, complete MCP client/server profile, official
conformance report, or A2A support has shipped. Their exact boundaries are
documented in [AgentService v1](agent-service.md) and the
[strict MCP Remote Tool profile](mcp-remote-tool.md).

## Product statement

StateKnot v1 is a Rust-native library and deployable runtime for typed,
durable, observable, and policy-enforced agent execution. It provides a direct
agent API for common tool-calling loops and a deterministic graph API for
long-running, concurrent, interruptible workflows.

The supported production runtime uses PostgreSQL as its durable source of truth
and exposes first-class MCP and A2A interoperability through versioned adapters.
Wire-protocol types do not define the stable StateKnot domain model.

## Supported v1 surface

### Core and orchestration

- typed text, image, audio, structured data, and file-reference content;
- versioned message, artifact, model, tool, agent, run, event, identity, and
  budget contracts;
- model streaming, tool calling, structured output, capability negotiation,
  cancellation, deadlines, and bounded retries;
- a prebuilt agent loop with instructions, tools, policy, limits, memory input,
  and typed final output;
- deterministic sequential, conditional, parallel/join, loop, subgraph, and
  pause/resume graph execution;
- human approval and external-input interrupts with action-bound, expiring
  resolution tokens.

### Durability and operations

- PostgreSQL 16 or later for runs, events, checkpoints, node attempts, leases,
  tool invocation records, interrupts, and transactional outbox records;
- S3-compatible object storage for artifacts and payloads above the documented
  inline threshold;
- multi-worker scheduling with tenant admission control, weighted fairness,
  lease/fencing protection, graceful drain, and crash recovery;
- explicit graph, state-schema, provider, tool, prompt, and policy versions;
- schema migrations, retention, garbage collection, backup/restore, and
  disaster-recovery procedures;
- OpenTelemetry traces and metrics, immutable audit events, redaction, budgets,
  deterministic test doubles, evaluation, and fault injection.

### Integrations and protocols

- OpenAI Responses/OpenAI-compatible and Anthropic as the two first-party model
  adapter families;
- local Rust tools plus MCP client and server support for the declared
  `2026-07-28` compatibility profile;
- A2A `1.0` client and server support for HTTP+JSON/REST and JSON-RPC, including
  Agent Card, messages, tasks, artifacts, streaming, cancellation,
  subscriptions, and reliable push delivery;
- an authenticated HTTP API and resumable SSE event stream for submitted runs;
- OIDC/JWT validation through configured issuers and JWKS, tenant-aware
  authorization, least-privilege delegation, and deny-by-default policy.

### Distribution

- embeddable Rust crates with a controlled `stateknot` facade;
- a Linux OCI image supporting `api`, `worker`, `scheduler`, and `all-in-one`
  roles while retaining the same PostgreSQL durability semantics;
- source archives, crates.io packages, OCI provenance, SBOMs, signatures,
  Apache-2.0 notices, compatibility matrices, and operator documentation.

## Explicit v1 exclusions

The following are not v1 deliverables and MUST NOT receive placeholder crates,
public traits, features, configuration keys, or compatibility promises:

- built-in RAG ingestion, vector database adapters, document chunking, or a
  connector catalog; retrieval remains an ordinary local or MCP tool;
- time-travel and run-fork APIs beyond production-required pause/resume;
- a third first-party model provider or a general provider marketplace;
- A2A gRPC or SLIMRPC bindings;
- AG-UI, MCP Apps, Agent Skills, A2UI, AGNTCY/SLIM, or AP2 adapters;
- Restate, Temporal, SQLite, or in-memory production durability backends;
- a YAML/JSON workflow DSL, visual workflow editor, or low-code builder;
- an in-process plugin marketplace or a built-in arbitrary-code sandbox;
- autonomous discovery and trust of arbitrary public agents;
- a hosted control plane, billing system, or model training platform.

An excluded feature may be proposed after a production use case demonstrates
that the existing tool, protocol, or application boundary is insufficient.

## Non-negotiable guarantees

StateKnot v1 cannot be released unless the following guarantees are supported
by executable evidence:

1. Given the same graph version, checkpoint, and recorded external results,
   scheduling and state reduction produce the same committed result.
2. Recovery never repeats a committed model request, node update, or tool result.
3. Database state transitions commit atomically and reject stale worker writes
   with fencing. External side effects use at-least-once invocation,
   idempotency keys where supported, and an explicit `unknown` outcome where
   the real-world result cannot be proven.
4. Every durable record, index, query, cache key, event stream, and artifact
   authorization path is tenant-scoped.
5. Authentication and authorization occur before resource-existence disclosure.
6. Cancellation, deadlines, budgets, backpressure, redaction, and audit context
   propagate through model, tool, graph, protocol, and storage boundaries.
7. Protocol support is claimed only for a version profile that passes its
   official conformance suite or TCK without unexplained required failures.
8. Supported database upgrades, rollback limits, retention behavior, RPO/RTO,
   and backup restoration are documented and tested.

## Supported deployment baseline

- Rust MSRV: `1.88.0`, required by the official MCP Rust SDK 3.x adapter;
- production OS: Linux containers on x86-64 and arm64;
- development and library CI: Linux, macOS, and Windows;
- PostgreSQL: versions `16` and `17` for the first release qualification
  matrix;
- object storage: S3-compatible API with TLS, bounded objects, checksums, and
  tenant-aware access control;
- TLS: Rustls-based clients and servers with certificate verification enabled.

The `all-in-one` role is a deployment convenience, not an in-memory mode. It
uses the same database schema, journal, leases, and recovery rules as separated
production roles.

## Qualification scenarios

The v1 claims are measured through three normative scenarios:

1. [Internal tool agent](scenarios/001-internal-tool-agent.md)
2. [Long-running approval and recovery](scenarios/002-long-running-approval.md)
3. [Cross-organization A2A collaboration](scenarios/003-cross-organization-a2a.md)

The shared load environment, measurement rules, and evidence requirements are
defined in the [scenario index](scenarios/README.md). A release candidate must
pass all three scenarios; a successful happy-path example is insufficient.

## Crate boundary rule

The repository starts with the `stateknot` facade. During the first vertical
slice, only proven dependency and semantic boundaries may become these crates:

- `stateknot-core`;
- `stateknot-runtime`;
- `stateknot-integrations`;
- `stateknot-server`;
- `stateknot-testkit`; and
- the `stateknot` facade.

Further splitting requires evidence of an independent dependency, compilation,
security, release, or semantic-versioning boundary and therefore requires an
RFC.
