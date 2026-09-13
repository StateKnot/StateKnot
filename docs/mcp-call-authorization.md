<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Input-aware authorization for durable MCP writes

[中文](mcp-call-authorization.zh-CN.md)

Use `McpRemoteTool::connect_authorized` for a remote Tool that performs writes.
It combines the existing [durable MCP adapter](mcp-remote-tool.md) with a
reviewed raw descriptor pin and mandatory per-attempt host policy. This is an
operable restricted binding, not the general effectful Graph Worker API or a
production release of StateKnot. Never use `McpComputeNode` for side effects.

## Binding and authority

Supply the same local Tool descriptor, exact endpoint/server identity, schema
registry and bounded HTTP options as `connect`. Replace its context-only
credential provider with `McpToolApproval::new(reviewed_digest, authorizer)`.
The authorizer implements `McpToolAuthorizer`:

- `resolve_startup` supplies discovery-only credentials for the frozen binding.
- `authorize_call` receives immutable references to the exact local descriptor,
  validated input, destination, remote name, reviewed digest and `ToolContext`.
  It must check tenant, trusted Run principal/consent, capability, actual target
  arguments and current revocation policy before resolving a scoped credential.
  Return `PermissionDenied` or `Unavailable` to prevent dispatch. There is no
  default allow, cached grant, anonymous fallback or parameter rewriting.

`ProviderEndpoint` supports exact equality against operator-configured endpoints;
it intentionally redacts its address in Debug. The authorization request's
Debug omits arguments, endpoint, tenant and credentials. Policy code must also
avoid logging input, secrets or raw errors. Never authenticate callers from
request-supplied tenant/Run IDs; this API accepts only trusted embedding code.
Resolve identity and consent from host-owned provenance/policy storage.

Retain a reviewed complete raw MCP Tool JSON object in the deployment manifest.
Compute `mcp_tool_descriptor_digest(&manifest_tool)` offline. Startup verifies
its RFC 8785 SHA-256 **before SDK decoding can drop unknown extensions**. Name,
description, annotations, schemas, metadata and extension changes affect the pin.
Do not populate the expected pin using live discovery. A pin is not executable
attestation, and annotations do not prove idempotency or authorization.
Header promotion (`x-mcp-header`) and non-forbidden Task execution are refused.

## Execution and recovery contract

The durable executor records the physical attempt start before authorization.
The adapter requires that origin event, validates binding and input locally,
acquires its call gate, then invokes current policy under the remaining attempt
deadline. Only approved exact arguments and the returned MCP credential reach
the endpoint. Run/tenant/fence, budget, journal, database credentials and policy
objects are not added to MCP metadata. Arguments are an explicit data-release
boundary: if the application puts secrets in an approved argument, they are sent.

Denied/unavailable authorization, invalid input and policy-only timeout occur
before HTTP call dispatch. Writes retain `ToolExternalEffect::NotStarted`.
Denial is non-retryable; policy unavailability may be retried only through the
existing ledger's explicit retry rules. Attempts still count as attempts; this
does not mean an external write occurred or was billed.

Once dispatch starts, the adapter sends at most one `tools/call`, without
redirect/reconnect/challenge/protocol-version replay. A lost response or timeout
retains `Unknown` and `ReconcileFirst`; HTTP status does not prove absence of an
external effect. Retrying the same durable handoff returns its retained state
without another policy call, network call or ledger revision. Authoritative
reconciliation is fenced and idempotent through the existing trusted API. It
does not call the remote write again, and is not an unauthenticated evidence API.

If a dispatched exchange fails, returns an invalid result, or its Future is
dropped, the approved connection is retired before releasing its gate. This
prevents queued SDK work from using a subsequent attempt's credential. Later
calls fail before policy evaluation with `authorization.binding_retired`.
Rebuild the same reviewed binding for **new authorized work**, not to replay an
unknown write. Pre-dispatch policy failures do not retire the binding. A
successful validated result allows reuse, with fresh policy on every new call.

The existing `connect` remains an explicit compatibility path and does not gain
input-aware authorization or the approved-connection retirement guarantee.

## Deployment and qualification

Deploy the trusted executor/registry with host-only database access. The remote
service needs independent OS/network isolation and an audience/resource-scoped,
short-lived MCP credential. Its server must independently verify that credential
and enforce access to the real target resource. Do not pass control-plane or
upstream provider tokens through the Worker. Use HTTPS; loopback HTTP fixtures
are not a public deployment. Revocation can race after a decision: bound token
lifetime and enforce revocation at the receiving resource, rather than claiming
an atomic transaction across policy, PostgreSQL and a remote business service.

Automated contract tests cover input/tenant denial, policy unavailability and
timeout, queued revocation, raw extension drift, unsafe extensions, dropped
Futures, dispatched timeout, secret-safe formatting and exact wire arguments.
Required PostgreSQL 16/17 CI tests verify authorization observes the committed
start, durable denial, lost response, duplicate suppression and idempotent
reconciliation. `mcp-authorization-postgres-*` artifacts retain both evidence
records plus exact source/tree/lock/environment information for 30 days.

No migration or new dependency is required. External currency settlement,
remote resource fencing, provider exactly-once effects and the general effectful
Worker API remain separate release gates. A later additive
[authenticated inline-success reconciliation Tool](mcp-reconciliation.md) now
provides the restricted operations ingress; it does not accept arbitrary errors
or artifacts. This dispatch profile does not claim to complete those gates.

Protocol references: [MCP Tool specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/tools.mdx)
and [MCP security guidance](https://modelcontextprotocol.io/docs/2025-11-25/tutorials/security/security_best_practices).
