<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0005: MCP compute Worker boundary

- Status: Draft (implemented qualification profile; not a release acceptance)
- Authors: StateKnot contributors
- Created: 2026-09-13
- Supersedes: none

## Summary and motivation

Keep durable orchestration in a trusted service and delegate repeatable pure
Graph computation to a separate MCP server. The existing schema-24 runtime
database role is deliberately too powerful for an untrusted process. Do not
weaken that boundary by sending its credentials, a RunFence, or a writable
checkpoint to a remote Worker.

This profile is a permanent restricted execution option, not an RPC wrapper
around `PostgresStore` and not the eventual effectful Worker/control-plane API.
It uses the existing MCP 2026-07-28 transport, authentication, resource limits
and local offline schemas. It does not introduce another wire protocol.

## Goals and non-goals

Provide an executable `McpComputeNode` with explicit data minimization, pinned
deployment descriptors, host-selected results, bounded single-exchange calls,
and real PostgreSQL/process recovery evidence. A deployment may use it for
deterministic parsing, normalization or application calculations.

Do not use it for models, billable API calls, side effects, mutable external
reads, MCP sampling/elicitation, children, arbitrary graph control or durable
leases on a remote process. Those require durable invocation accounting and a
separately qualified authority API. No new SQL grants or sandbox are claimed.

## User-facing design

The [English](../mcp-compute-worker.md) and
[Chinese](../mcp-compute-worker.zh-CN.md) guides describe the deployment contract.
`crates/stateknot/tests/mcp_compute.rs` and its PostgreSQL module compile the
complete binding, remote execution and recovery path. The facade combines
existing runtime and integration crates; neither lower-level crate acquires a
dependency on the other.

The operator supplies an exact compiled Graph/node, explicit top-level state
field allowlist, exact input SchemaReference, fixed output mode, expected
server implementation revision and independently reviewed complete Tool
descriptor SHA-256. Startup rejects missing/changed descriptors, local schema
disagreement, rejected catalog entries, promoted input headers and Tasks.
Endpoint authentication is supplied by the existing immutable `McpClient`;
serverInfo is a version assertion, not cryptographic identity.

## Detailed semantics

1. The Driver verifies the Run/checkpoint and commits the physical node start.
2. The adapter checks the local Graph/node context and projects only selected
   top-level state fields. Missing fields or schema-invalid projected input
   fail before any `tools/call`. Empty selection means `{}`, not full state.
3. `McpClient::call_tool_once` sends at most one HTTP exchange, with redirects,
   generic retries, protocol recovery and OAuth challenge replays disabled.
   Authorization resolution and queueing are inside the request deadline.
4. Only complete structured JSON with an empty content list is accepted. Remote
   errors, interactive responses, notifications, unknown result-envelope fields,
   invalid output and excess bytes fail closed without public payload logging.
   Bounded protocol `_meta` is ignored; it grants no authority.
5. Local frozen schemas validate output. The host constructs an update plus
   Continue, or a Terminal output according to its startup binding. It creates
   no invocation bindings and takes no remote usage/failure/identity claims.
6. The existing Driver rechecks cancellation and exact live lease, constructs
   its own pending intent, then commits under database fencing. Replay consumes
   committed pending results even when the remote Worker is no longer running.

The Worker may repeat after a host process crash before durable completion;
repeatability/no-effects is an operator-verified execution contract, not an
annotation or transport guarantee. An observed failure is recorded with
`RetryAdvice::Never`; the adapter never schedules its own retry. Cancellation
stops local waiting; it does not prove remote computation stopped. Driver and
client concurrency limits bound active work and wait time. The adapter uses
the existing journal-isolated scheduling contract and does not support Join.

Normalized invocation usage is zero because the profile authorizes no model,
Tool-ledger or paid work. This is not free infrastructure: deployers must bound
CPU/memory/concurrency and account for their service hosting independently.

## Persistence, migration and compatibility

No SQL migration or persisted format changes. Existing node-start, pending
result, failure, checkpoint, Run fencing and replay records remain authoritative.
MCP JSON-RPC IDs are exchange correlation only and do not identify durable
attempts. A new Graph revision is required when changing Worker code,
projection, schema or effects policy, just as for a local executable binding.
Descriptor pins must come from a retained release manifest, not current
untrusted discovery. Keep old executable deployments available while draining
old Runs. Do not roll back by silently rebinding an existing Graph revision.

Rust 1.88 remains the MSRV. The facade gains only existing workspace dependency
edges; no third-party package is added. The general MCP client's existing
`call_tool` recovery behavior is unchanged; `call_tool_once` is additive.

## Security and privacy

The trusted host authenticates and authorizes Agent admission before execution.
Its Worker binding is a deployment-time data-release policy. Selected objects
include their nested fields, so operators must not select secret-bearing
containers. The adapter never serializes GraphNodeContext, tenant/principal,
Run/attempt/fence, budgets, journal, deployment registry or database credentials.

Production uses HTTPS with verified server identity, a resource-specific MCP
credential which cannot authenticate to the control plane, independent Worker
OS identity/filesystem and no route/credentials to PostgreSQL, control-plane
administration, provider secrets or cloud metadata. These infrastructure rules
are deployment prerequisites; the library cannot enforce a remote firewall.
Loopback HTTP is for controlled local fixtures/sidecars only. Process separation
and `env_clear` evidence alone are not a sandbox claim.

Output validation proves shape and authorized destination, not computation
correctness. A compromised Worker can lie within its output schema or retain
data it was explicitly given. Only expose computations and data for which that
trust level is acceptable. Remote tool annotations are untrusted hints, as the
[MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/tools.mdx)
requires; they do not establish effect or isolation policy.

## Operations and observability

Fail startup on discovery/pin drift; do not substitute another Worker. Use
existing node-attempt journal events and fixed `worker.*` failure codes for
correlation. Public errors contain neither remote message nor raw response.
Do not log projected input, bearer values or Worker result bodies at a proxy.
Remote unavailability cannot lose a committed result; uncommitted computation
may need recomputation under the existing recovery rules. This adds no RPO/RTO,
failover, capacity or production-support guarantee.

## Alternatives and qualification

Giving the runtime SQL role to the Worker violates the desired boundary.
Implementing a second custom HTTP compute protocol duplicates existing MCP
authentication, limits and conformance work. Treating a side-effecting MCP Tool
as pure computation would bypass the invocation ledger and is prohibited.

Qualification covers explicit projection; descriptor/schema/extension refusal;
hostile result, JSON-RPC ID, auth/protocol-replay and size limits; cancellation;
a credential-free owned process using the first-party MCP server; fresh
registries, noninitial and pending-result replay with that process stopped;
and real stale-fence/hostile-result rejection by PostgreSQL.

The general effectful Worker API, worker-only SQL procedures, immutable
executable release attestation, infrastructure network isolation qualification,
remote CPU metering, full child profile and production capacity gates remain
open. This RFC remains Draft until the broader design/release review accepts
its contract; implementation evidence is not a review waiver.
