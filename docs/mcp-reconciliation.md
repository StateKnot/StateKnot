<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Authenticated MCP Tool result reconciliation

[中文](mcp-reconciliation.zh-CN.md) · [RFC-0006](rfcs/0006-authenticated-tool-result-reconciliation.md)

`McpToolReconciler` is a privileged operations Tool for resolving an exact
`Unknown` Tool attempt with an authoritative inline successful result. It never
calls the provider. This is an executable restricted profile, not a production
release, generic Worker API or exactly-once external-write guarantee.

## Host wiring

1. Register `McpToolReconciler::audit_schema()` in the offline
   `JsonSchemaRegistryBuilder`: its `$id`, version `1.0.0`, and SHA-256 of RFC 8785
   canonical bytes form its `SchemaReference`. Also retain all original Tool
   output schemas. Build the registry and fail startup on any mismatch.
2. Implement `McpReconciliationAuthorizer`. Map an already authenticated,
   issuer-namespaced MCP subject to trusted tenant/principal; check the exact
   Run, invocation, attempt and result against authoritative business evidence.
   Return `McpReconciliationGrant` only after that check. Retain the referenced
   policy and evidence/decision artifacts immutably outside the request.
3. Construct `McpToolReconciler::new(store, schemas, Arc::new(authorizer))` with
   the host-only `PostgresStore`. Register it with its exact `definition()` in
   `McpServerToolRegistryBuilder`, then use the existing `McpServerToolService`
   and `McpServerHttpService` with bearer authentication and private caching.
4. Expose through verified HTTPS, exact Host/Origin policy and a dedicated
   operations audience. Set outer request/proxy deadlines above the inner
   15-second deadline; bound rate, concurrency, request size and DB pool usage.
   Use the [trusted SQL role profile](postgresql-roles.md), never remote SQL
   credentials or an untrusted-worker runtime role.

The [compiled end-to-end fixture](../crates/stateknot/tests/mcp_reconciliation.rs)
demonstrates complete registration, HTTP calls, PostgreSQL records and recovery.
Its static tokens and permissive evidence policy are **test-only**; use your
identity provider and domain evidence verifier in a deployment. There is no
production default authorizer. A scope alone does not prove a write succeeded.
For example, verify the provider's retained operation receipt against the exact
original target/attempt, not a Worker-supplied claim or live discovery metadata.

Never advertise this Tool in the ordinary Agent or remote Worker catalog. Its
dedicated `stateknot:reconcile-result` scope is checked both by the Tool service
and the handler. Every retry repeats current evidence/resource authorization
before any DB lookup; revoked callers cannot read an earlier successful receipt.
Issuer namespacing and tenant mapping are authenticator/host responsibilities,
not fields a remote caller can select.

## Submission and response

Call `tools/call` for `stateknot_reconcile_tool_result_v1`, using the existing
[MCP client](mcp-client.md) with a scoped credential. Arguments are exactly:

| Field | Meaning |
|---|---|
| `event_id` | UUIDv7 generated once for this logical evidence submission |
| `run_id`, `invocation_id`, `attempt_id` | Original target and physical Tool attempt, not a new execution |
| `expected_revision` | Original Unknown revision, canonical decimal **string** within signed 64-bit range |
| `expected_digest` | Its exact `sha256:<64 lowercase hex>` record digest |
| `output` | Inline JSON result satisfying the original locally pinned Tool contract |

The target comes from authorized host operations state, not public enumeration.
Unknown fields are rejected. Tenant, principal, fence, schemas, policy, artifacts
and arbitrary errors cannot be supplied. The host constructs provenance from the
frozen ledger descriptor and validates the result before mutation.

Success has empty `content` and `structuredContent` containing `event_id`,
`invocation_digest` and decimal-string `revision`. It contains no result body.
The authorization audit and next Committed revision are one fenced transaction.
The audit stores request, policy and decision digests plus trusted principal/
policy identities; it does not duplicate raw output or credentials. Sensitive
output still resides in the existing invocation ledger: protect backups and
storage access. A digest is not a replacement for retained evidence or proof of
its truth.

## Retry and incident runbook

Preserve the original request values and event ID, including after process
restart. JSON whitespace/key order is immaterial; the canonical request digest
also binds authenticated subject and mapped caller. A changed output, subject,
target or event ID is not the same submission. Refreshing a token is compatible
only when trusted subject/tenant/principal identity remains the same.

| Result | Action |
|---|---|
| `reconciliation.denied` | Stop; correct authorization/evidence. No pre-policy existence lookup. |
| `reconciliation.invalid` | Correct the pinned contract problem; do not blindly replay a write. |
| `reconciliation.conflict` | Inspect authorized host records; attempt/head/event binding differs. |
| `reconciliation.busy` | Another live lease or bounded journal contention; back off and retry the **same** submission. |
| `reconciliation.unavailable`, timeout or lost HTTP response | Outcome may already be committed. Retry the **same** submission with bounded backoff; never regenerate the event ID or re-execute the write. |

Handler failures are MCP Tool results with `isError: true` and only a fixed code.
Authentication, protocol, input-shape and service admission failures may instead
be HTTP/JSON-RPC errors; do not interpret them as proof of an external effect.
Use bounded retries, then operations escalation, not infinite retry loops.

A live Worker is never forcibly superseded. The host claims a fresh lease only
for a new reconciliation and releases only its own fence. Cancellation/timeout
can leave it until configured database-clock expiry. Once committed, identical
receipt recovery needs no lease, adds no journal/revision, and works under a
different live Worker lease or after recreating the service. Current policy is
still required. Keep schema/evidence/ledger retention aligned with this window.

## Evidence and boundaries

Run the integration fixture against a disposable database:

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 STATEKNOT_TEST_DATABASE_URL='postgres://…' \
  cargo test -p stateknot --test mcp_reconciliation --locked -- --nocapture --test-threads=1
```

Required PostgreSQL 16/17 CI verifies real HTTP auth, pre-lookup policy, fenced
atomic audit, client disconnect after commit before delivery, 24 concurrent
first submissions and 24 duplicate reads, conflicts and fresh-service recovery.
`mcp-reconciliation-postgres-*` artifacts retain the machine-readable evidence
and source/tree/lock/environment metadata for 30 days. This fixture does not
call a real business provider or prove that a particular external write occurred.

No migration or new third-party version is introduced. The additive exact
revision getter verifies the direct predecessor and journal anchor, not full
history. Error/artifact reconciliation, provider settlement/fencing, general
Worker execution, infrastructure isolation and capacity/failover qualification
remain separate gates. See [authorized writes](mcp-call-authorization.md) for
the dispatch-side boundary.

Protocol references: [MCP Tools](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/tools.mdx)
and [MCP security guidance](https://modelcontextprotocol.io/docs/2025-11-25/tutorials/security/security_best_practices).
