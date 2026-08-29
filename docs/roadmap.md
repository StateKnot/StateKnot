<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot roadmap

> Current phase: M0 architecture contracts. Repository bootstrap is complete;
> StateKnot has no production release or stable API yet.

The roadmap is evidence-driven. Dates may be estimated in planning discussions,
but a milestone exits only when its acceptance evidence is committed or linked
from the repository.

## Current M0 tracking

- [x] Freeze the [v1 scope and explicit non-goals](v1-scope.md).
- [x] Define the three [qualification scenarios](scenarios/README.md), shared
  reference environment, loads, failure matrices, and release thresholds.
- [ ] Accept RFC-0001 for the core domain and capability model.
- [ ] Accept RFC-0002 for deterministic graph and scheduler semantics.
- [ ] Accept RFC-0003 for PostgreSQL durability, recovery, and migration.
- [ ] Accept RFC-0004 for MCP/A2A identity and security mapping.
- [x] Validate the first protocol-neutral run lifecycle, interrupt/timer wait,
  cancellation-race, terminal-outcome, schema, property, and wire contracts in
  the unpublished `stateknot-core` crate.
- [x] Validate RFC 8785 payload bytes, journal append identity/head/hash-chain,
  lease renewal, fencing epoch, stale-attempt, schema, property, and wire
  contracts in the unpublished `stateknot-core` crate.
- [ ] Compile the four public contract examples against the proposed APIs.
- [ ] Commit the benchmark harness and fault-injection matrix.

## M0 — Architecture contracts

Deliverables:

- three golden scenarios: an internal tool agent, a long-running approval flow,
  and cross-organization A2A collaboration;
- explicit load, latency, data-size, tenancy, and failure assumptions for each
  scenario;
- accepted RFCs for the core domain, deterministic graph execution,
  PostgreSQL durability, and MCP/A2A identity and security mapping;
- a recorded decision excluding built-in RAG ingestion and vector database
  adapters from v1;
- reference hardware and measurable performance/recovery thresholds.

Exit criteria:

- example APIs compile against proposed contracts;
- state transitions and external-side-effect guarantees are unambiguous;
- schema migration, retention, RPO/RTO, authentication, policy, and scheduler
  fairness have executable acceptance plans;
- unresolved questions capable of changing public types are closed.

The scope decision excludes built-in RAG ingestion and vector database adapters
from v1. Retrieval remains available through ordinary local or MCP tools.

## M1 — Production-shaped vertical slice

Build one complete path:

```text
HTTP/SSE -> durable typed graph -> model -> MCP tool -> checkpoint
         -> approval interrupt -> resume -> A2A agent -> artifact
```

The slice must use PostgreSQL, survive process termination at every persistence
boundary, reject stale worker writes through fencing, and reuse committed model
and tool results after recovery. Fake-model tests run on every pull request;
live-provider tests run only through controlled credentials and budgets.

Exit criteria include committed fault-injection results and the applicable MCP
and A2A conformance reports. A happy-path demo alone does not complete M1.

## M2 — Stable core and runtime

- typed content, model, tool, agent, and run contracts;
- deterministic sequential, conditional, parallel/join, loop, subgraph, and
  pause/resume semantics;
- PostgreSQL journal, checkpoints, pending writes, leases/fencing, invocation
  ledger, outbox, migration, and retention;
- OpenAI-compatible and Anthropic adapters;
- testkit, policy, budgets, cancellation, deadlines, and OpenTelemetry.

## M3 — Protocol and server readiness

- MCP client/server support for the declared version profile;
- A2A REST/JSON-RPC client/server, Agent Card, task, artifact, stream, cancel,
  subscription, and reliable push behavior;
- authenticated HTTP/SSE API, worker and scheduler roles, graceful drain,
  health/readiness, backup/restore procedures, and tenant isolation;
- published compatibility matrix and security test evidence.

## M4 — Release candidate

- all release gates in the implementation plan pass;
- cross-platform/MSRV CI, soak and failure tests, migration tests, SBOM,
  provenance, signed artifacts, and third-party notices are verified;
- at least two distinct production pilots validate the supported scope;
- API, support, deprecation, vulnerability-response, and release policies are
  documented for the candidate version.

## Explicitly deferred

Until real demand proves otherwise, v1 does not include time-travel/fork APIs,
a third model provider, a second vector database, AG-UI/MCP Apps/A2UI adapters,
AGNTCY/SLIM/AP2 integration, alternative durable runtimes, a plugin market, a
visual workflow editor, or a built-in code-execution sandbox.
