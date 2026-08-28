<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# GS-001: Internal tool agent

> Status: M0 baseline<br>
> Owner: StateKnot runtime and integrations<br>
> Primary execution mode: embedded SDK and submitted HTTP run

## Purpose

An authenticated employee asks an internal assistant to investigate a service
incident, query read-only operational systems, propose a remediation, obtain
approval for a controlled write, perform that write, and return a typed report.

This scenario proves that the common Agent API is usable without manually
constructing a graph while preserving the same durable, policy, budget, and
audit semantics as the graph runtime.

## Actors and trust boundaries

- an employee authenticated by the configured OIDC issuer;
- a StateKnot API, scheduler, and worker pool serving 100 tenants;
- one configured model provider;
- three read-only tools and one approval-gated write tool;
- one MCP server containing both trusted tool metadata and untrusted tool data;
- PostgreSQL and S3-compatible artifact storage.

Model output, tool metadata received over MCP, tool results, incident text, and
artifact content are untrusted. Authentication claims, static policy, tool risk
classification, budgets, and approval rules are outside model control.

## Workflow

1. The client submits a typed incident request with an idempotency key.
2. StateKnot authenticates the principal, resolves tenant policy, persists the
   run, and begins a resumable event stream.
3. The model selects and invokes read-only tools through local and MCP adapters.
4. Tool inputs are schema-validated and policy-checked before invocation.
5. A proposed write becomes an approval interrupt containing the exact action,
   parameters, risk, provenance, and expiration.
6. An authorized approver resolves the interrupt. The worker executes the write
   with a stable idempotency key and records its real-world outcome.
7. The model produces a typed incident report containing evidence references,
   actions taken, remaining risk, and usage totals.

## Workload profile

The release test uses deterministic providers and tools:

| Dimension | Profile |
|---|---:|
| Tenants | 100 |
| Sustained run starts | 25/s for 30 minutes |
| Burst | 75/s for 60 seconds |
| Concurrent active runs | 500 |
| Model turns per run | 2–4 |
| Tool calls per run | 3–6, at most one write |
| Stream events per run | p95 <= 400, hard limit 2,000 |
| Input message | p95 <= 32 KiB, hard limit 256 KiB |
| Inline tool result | p95 <= 64 KiB, hard limit 256 KiB |
| Artifact | p95 <= 5 MiB, hard limit 25 MiB |
| Run deadline | 10 minutes |

One noisy tenant supplies 50% of submitted load while holding only 20% of the
configured scheduler weight. Ten other tenants continuously submit small runs
to measure starvation and queue isolation.

## Required budgets and policy

Every run has limits for model turns, input and output tokens, tool calls, write
calls, wall time, concurrency, artifact bytes, and known provider cost. Crossing
a hard limit produces a typed terminal error and an auditable budget event; it
must not silently truncate a write action or continue with unbounded retries.

The write tool requires a principal with the configured approval scope who is
not the model. The approval token binds tenant, run, tool identity and version,
canonical argument hash, approver, policy version, expiry, and nonce.

## Failure matrix

| Injection | Required behavior |
|---|---|
| Provider returns 429 with retry metadata | Apply bounded backoff within deadline and budget; emit retry evidence |
| Provider times out after request acceptance | Classify the attempt; do not claim the provider did not process it without evidence |
| Provider emits malformed structured output | Run only the configured bounded repair attempts, then fail with diagnostics |
| MCP server changes a tool schema mid-run | Keep the run-pinned tool version or stop with an explicit compatibility error |
| Tool returns a schema-invalid result | Reject it as untrusted data and preserve the raw response only under redaction policy |
| Worker dies before a tool call | Recover and invoke once when still eligible |
| Worker dies after tool effect but before commit | Resolve through idempotency/query when possible; otherwise enter `unknown`, never blind retry |
| Worker dies after invocation commit | Reuse the committed result without invoking again |
| Database connection is unavailable | Stop external actions without a durable intent record and resume after storage recovery |
| SSE client is slow or disconnects | Bound memory, disconnect when necessary, and permit replay from the last committed event ID |
| Approval token is replayed or altered | Reject without exposing run details and emit a security audit event |

Every worker-termination case runs at every durable boundary around model,
tool, interrupt, event, checkpoint, and terminal-result commits.

## Acceptance criteria

In addition to the [shared objectives](README.md#shared-service-objectives):

- 99.9% or more of non-injected runs reach their expected typed terminal result;
- duplicate submission with the same tenant and idempotency key creates one run;
- no approved write produces more than one business effect when the tool honors
  its idempotency contract;
- no non-idempotent ambiguous write is automatically retried;
- reducer and final-output hashes match across at least 100 randomized task
  completion orders for the same recorded inputs;
- the noisy tenant cannot exceed its configured share while other eligible
  tenants wait;
- budget excess, policy denial, cancellation, and deadline produce distinct,
  stable error categories;
- ordinary telemetry contains no prompts, tool arguments, tool results,
  credentials, or approval tokens unless the test explicitly enables a
  redacted content-recording policy;
- the final report can trace every claim to a model response, tool invocation,
  artifact, policy decision, and graph/prompt/tool version.

## Required evidence

- compilable embedded and HTTP examples using the same Agent definition;
- fake-model golden traces and provider contract fixtures;
- property tests for budget accounting, reducer ordering, and idempotency;
- per-boundary crash/recovery report;
- tenant fairness and latency histogram output;
- security tests for tool injection, approval replay, and cross-tenant IDs;
- an operator-visible trace proving causation from request to final artifact.

## Non-goals

This scenario does not require autonomous access to arbitrary MCP servers,
unrestricted shell execution, built-in retrieval ingestion, or an assertion of
exactly-once behavior for an external system that offers no idempotency or
status-query mechanism.
