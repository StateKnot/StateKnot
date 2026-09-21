<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Authenticated known-effect Tool failure reconciliation

[中文](mcp-error-reconciliation.zh-CN.md) · [RFC-0007](rfcs/0007-authenticated-known-error-reconciliation.md)

`McpToolErrorReconciler` records authoritative failure evidence for an exact
Unknown write attempt without calling its provider again. It accepts only
confirmed `not_applied` or `applied` effects and commits a Failed revision.
This is a restricted profile in the exact `0.1.0-alpha.1` public preview, not
production acceptance.
The [successful-result Tool](mcp-reconciliation.md) remains separate and unchanged.

## Decide whether the evidence fits

- `not_applied`: the original intended write was not applied. Execution may have
  started and incurred charges; this is not proof of a safe or free retry.
- `applied`: the intended write was applied despite the failed outcome. This
  does not roll back or compensate that effect.
- Unknown, partial or contradictory effects: keep the attempt unresolved and
  escalate to the domain evidence process. Do not guess either accepted value.

The host fixes `RetryAdvice::Never`, `ToolErrorPhase::Execution` and origin
`mcp.reconciliation`; it derives provenance from the original frozen descriptor.
There is no provider dispatch, automatic retry, Run closure or usage settlement.
The downstream runtime still needs its ordinary failure/accounting evidence.

## Wire the trusted host

1. Register `McpToolErrorReconciler::audit_schema()` in the offline schema registry
   using its `$id`, version `1.0.0` and SHA-256 of RFC 8785 canonical bytes. Startup
   rejects a missing/drifted schema. Retain success-v1 schemas if that Tool is used.
2. Implement the independent `McpErrorReconciliationAuthorizer`. From a verified,
   issuer-namespaced MCP subject, map the tenant/principal and verify permission
   for this exact Run/invocation/attempt plus authoritative failure/effect evidence.
   Approve the failure message for public Run/Agent surfaces. Return a
   `McpReconciliationGrant` binding the retained policy and evidence digests.
3. Construct `McpToolErrorReconciler::new(store, schemas, Arc::new(authorizer))`;
   register its `definition()` and handler with `McpServerToolRegistryBuilder`.
   Use the existing authenticated `McpServerToolService`/`McpServerHttpService`.
4. Grant `stateknot:reconcile-error` only to reviewed operations identities on a
   dedicated HTTPS audience. `stateknot:reconcile-result` grants no failure rights
   and the error scope grants no success rights. Keep both out of ordinary
   Agent/Worker catalogs; never provide remote callers SQL credentials or fences.
5. Apply exact Host/Origin policy, private caching, bounded body/concurrency/rate
   and DB pool limits. Outer deadlines must allow the inner 15-second operation.
   Use the [trusted SQL role profile](postgresql-roles.md), not superuser access.

The [compiled registration and HTTP fixture](../crates/stateknot/tests/mcp_reconciliation.rs)
and [failure qualification](../crates/stateknot/tests/mcp_reconciliation/error.rs)
show the complete integration. Their static tokens, evidence policy and database
transport settings are test-only. No production allow-all policy is supplied.
An HTTP error or Worker assertion is not evidence: verify the retained provider
operation identity and business effect against the original attempt. Keep the
policy/decision artifacts immutably; a digest alone does not prove truth.

## Submit one immutable evidence request

Call `tools/call` for `stateknot_reconcile_tool_error_v1`. Fields are exactly:

| Field | Contract |
|---|---|
| `event_id` | One UUIDv7 for this logical submission; preserve across retries |
| `run_id`, `invocation_id`, `attempt_id` | Exact original target, not a new execution |
| `expected_revision` | Original Unknown revision, canonical decimal string within signed 64-bit range |
| `expected_digest` | Original `sha256:<64 lowercase hex>` record digest |
| `failure_id` | Stable UUIDv7 identifying this authoritative failure occurrence |
| `failure_category` | Core failure category except `ambiguous_external_outcome` |
| `failure_code` | Application code, at most 128 ASCII characters; lowercase segments separated by dots |
| `failure_message` | Nonempty, public-approved message, at most 1,024 UTF-8 bytes, no control characters |
| `external_effect` | Exactly `not_applied` or `applied` |

Unknown fields, including null values for tenant, fence, retry advice, phase,
origin, provenance, output, artifacts, usage, details or recovery handle, are
rejected. The typed decoder also validates UUIDv7, digest and numeric bounds.
The host supplies provenance and retry semantics; message shape validation does
not detect secrets. Never submit credentials, traces or private provider bodies.

Success has empty MCP `content` and exactly `event_id`, `invocation_digest` and
decimal-string `revision` in `structuredContent`. Here success means **the
evidence was recorded**, not that the original Tool succeeded. The Failed record
and `mcp-tool-error-reconciled` audit are atomic. Audit schema:
`https://stknot.com/schemas/runtime/mcp-tool-error-reconciliation/1.0.0`.
The audit stores identity/policy and digests, not raw failure text. The public
failure message remains in the invocation ledger; protect storage and backups.

## Lost responses, contention and incidents

Preserve the entire request, especially both stable IDs, original revision and
message. Canonical JSON binds tool name, authenticated subject, mapped caller
and all request fields; key order/whitespace do not matter. A token refresh is
compatible only if trusted subject/tenant/principal remain the same.

| Result | Action |
|---|---|
| `reconciliation.denied` | Stop and correct current permission/evidence; no pre-policy database lookup |
| `reconciliation.invalid` | Fix the contract/evidence problem; do not replay the business write |
| `reconciliation.conflict` | Inspect authorized records for target, evidence or winning-outcome differences |
| `reconciliation.busy` | Back off; do not force takeover of a live Worker |
| `reconciliation.unavailable`, timeout or disconnect | The commit may exist; retry only the identical request with bounded backoff |

Use bounded attempts/elapsed time, then escalate. Never regenerate the event or
failure ID to get around a conflict. Success/error submissions racing on the
same Unknown record have one authoritative winner; the other profile conflicts
even if it reuses the event ID. Current authorization runs before every duplicate
lookup, so revoked credentials cannot recover old receipts.

New commits claim and release only their own lease; cancellation can leave that
lease until database-clock expiry. An identical committed receipt needs no
lease or additional revision and works after restart or under a later Worker.
Keep historical schemas/evidence/ledger records for the full recovery window.
On rollback, disable new ingress but retain a compatible receipt reader and
the error audit schema; do not rewrite accepted failure records as successes.

## Verify the boundary

Against an isolated PostgreSQL 16 or 17 database:

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 STATEKNOT_TEST_DATABASE_URL='postgres://…' \
  cargo test -p stateknot --test mcp_reconciliation --locked -- --nocapture --test-threads=1
```

CI requires success, known-error and mixed-race evidence markers; artifacts
`mcp-reconciliation-postgres-*` retain logs and source/tree/lock/environment for
30 days. Tests cover both effects, scope/resource denial, lost HTTP delivery,
24 duplicates per effect, 24-way error and mixed-result races, revocation,
fresh-service recovery, stale fencing, schema drift and success-v1 compatibility.
They do not contact a live business provider or verify a real external effect.
No migration/dependency change is needed. Partial effects, artifacts, provider
settlement/fencing and general effectful Worker isolation remain separate gates.
