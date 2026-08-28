<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# GS-003: Cross-organization A2A collaboration

> Status: M0 baseline<br>
> Owner: StateKnot protocols, runtime, and security<br>
> Primary execution mode: A2A client and server over HTTPS

## Purpose

An enterprise procurement agent delegates a bounded supplier-assessment task to
an agent operated by another organization. The agents negotiate capabilities,
exchange messages and artifacts, stream progress, survive disconnects, deliver
push notifications at least once, and preserve end-to-end identity and scope.

This scenario proves interoperability without exposing either agent's internal
graph, memory, tools, or StateKnot-specific types.

## Actors and trust boundaries

- a user and originating enterprise agent;
- a StateKnot A2A client, server, and durable runtime;
- up to 50 independently operated remote A2A agents;
- organization-specific OIDC issuers, workload credentials, and allowlists;
- public DNS, HTTPS, redirects, webhooks, and artifact URLs;
- PostgreSQL outbox and S3-compatible artifact storage.

Remote Agent Cards, messages, status updates, artifact metadata, URLs, and push
endpoints are untrusted network input. Discovery does not imply authorization.

## Protocol profile

- A2A protocol version `1.0`;
- HTTP+JSON/REST and JSON-RPC bindings over HTTPS;
- Agent Card discovery at the standardized well-known location;
- messages, tasks, artifacts, streaming, cancellation, subscriptions, and push
  notification configuration;
- explicit version and extension negotiation;
- protocol wire models mapped to versioned internal domain types at the adapter
  boundary;
- gRPC, SLIMRPC, public directory discovery, and undocumented extensions are out
  of scope for this scenario.

## Workflow

1. The originating agent resolves an allowlisted Agent Card and validates URL,
   transport, version, authentication, and supported input/output modes.
2. Policy narrows the user's authority into a task-specific delegation with a
   deadline, budget, data classification, and permitted artifact types.
3. The client sends a message and receives either a terminal response or a task.
4. Task updates and artifacts arrive through streaming. Disconnects resume from
   durable StateKnot event state without creating a second business task.
5. A remote request for additional input maps to an internal waiting state and
   is resolved only by an authorized principal.
6. Push notifications are durably enqueued, authenticated, retried with bounds,
   and deduplicated by the receiver.
7. Cancellation races resolve according to recorded A2A and internal state,
   preserving ambiguity when a remote outcome cannot be proven.
8. The final A2A artifact is mapped to a StateKnot artifact with hash,
   provenance, media type, security label, and retention policy.

## Workload profile

| Dimension | Profile |
|---|---:|
| Remote organizations/agents | 50 |
| Sustained delegated task starts | 20/s for 30 minutes |
| Burst | 60/s for 60 seconds |
| Concurrent remote tasks | 500 |
| Updates per active task | p95 <= 5/s, hard limit 20/s |
| Task duration | 1 second to 24 hours |
| Inline message or artifact metadata | hard limit 1 MiB |
| Referenced artifact | p95 <= 100 MiB, hard limit 1 GiB |
| Redirects per fetch | at most 3, each revalidated |
| Push attempts | bounded by deadline, retention, and retry policy |

Ten remote agents intentionally behave slowly, five send duplicates or
out-of-order updates, and one allowlisted but compromised fixture attempts SSRF,
cross-tenant identifiers, extension confusion, and oversized payloads.

## Security requirements

- authenticate and authorize before revealing whether an Agent Card extension,
  task, message, artifact, subscription, or push configuration exists;
- validate TLS, issuer, audience, scopes, token expiry, delegation depth, and
  tenant binding on every applicable request;
- resolve DNS and check all addresses before connection, reject loopback,
  link-local, private, metadata-service, and policy-denied ranges, and repeat the
  checks after redirects and DNS changes;
- cap response size, decompressed size, redirect count, connection time, total
  time, stream buffers, update rate, artifact size, and content types;
- preserve extension URIs and unknown fields only according to the negotiated
  compatibility policy; never interpret unknown data as privileged commands;
- sign or authenticate push delivery according to the configured profile and
  bind delivery to task, tenant, destination, event, and expiry;
- never place workload credentials or delegated tokens into prompts,
  checkpoints, artifacts, ordinary logs, or remote error messages.

## Failure matrix

| Injection | Required behavior |
|---|---|
| Agent Card changes during a task | Continue with the pinned interface or fail explicitly; do not silently switch trust or transport |
| Unsupported A2A version | Return the protocol-defined version error without creating a run or task |
| Duplicate client message | Return/reuse the idempotent task result where the binding supports the configured key |
| Duplicate or out-of-order task update | Deduplicate and order by protocol/task rules without corrupting internal event sequence |
| Stream disconnect and reconnect | Continue the same task and project only new committed updates |
| Remote timeout after task acceptance | Query/reconcile the known task before creating another task |
| Cancellation crosses terminal completion | Preserve the first valid committed terminal state and expose the race in audit evidence |
| Push receiver returns 429/5xx | Retry through outbox policy without blocking task commit |
| Push succeeds but acknowledgement is lost | Deliver at least once; receiver deduplicates by delivery/event ID |
| URL redirects to a private address | Reject before connection and emit a redacted security event |
| DNS answer changes after validation | Revalidate the actual connection target and reject forbidden ranges |
| Oversized/decompression-bomb payload | Stop within configured byte and CPU limits without process-wide memory growth |
| Token has wrong audience or broader unapproved scope | Reject before task lookup or creation |

## Acceptance criteria

In addition to the [shared objectives](README.md#shared-service-objectives):

- StateKnot client and server pass all MUST requirements for the declared A2A
  `1.0` REST and JSON-RPC profiles in the pinned official TCK;
- cross-SDK tests pass against at least two independently implemented official
  SDKs for each claimed binding;
- 99.9% or more of non-injected task starts produce the expected protocol and
  internal task outcome;
- no injected duplicate creates a second committed internal action or artifact;
- push delivery loses zero committed notifications and intentionally may
  duplicate; duplicate receiver effects remain zero;
- stream interruption loses zero committed updates and produces no event-seq
  forks;
- all malicious URL, redirect, DNS, token, tenant, payload, extension, and replay
  fixtures are rejected at the correct boundary;
- remote backpressure cannot create unbounded tasks, connections, buffers,
  outbox records, or in-memory event queues;
- every internal run/event can identify the remote agent, Agent Card version,
  binding, protocol version, authenticated principal/delegation, and remote task
  or message identifier without logging credentials.

## Required evidence

- pinned A2A TCK report and compatibility matrix;
- cross-SDK REST and JSON-RPC transcripts with sensitive fields removed;
- property and fuzz tests for wire mapping, unknown fields, task-state mapping,
  duplicate/order handling, and content limits;
- SSRF, DNS rebinding, redirect, token-confusion, replay, and tenant-isolation
  security reports;
- stream reconnect and outbox delivery fault reports;
- load, backpressure, descriptor, memory, connection, and queue metrics;
- an operator runbook for remote ambiguity, incompatible Agent Cards, stuck
  subscriptions, push exhaustion, and compromised peers.

## Non-goals

This scenario does not establish automatic trust from public discovery, expose
internal StateKnot run identifiers as A2A task identifiers, require remote
agents to use StateKnot, or claim exactly-once network delivery.
