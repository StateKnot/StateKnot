<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0007: Authenticated known-effect Tool error reconciliation

- Status: Draft (implemented qualification profile; not release acceptance)
- Authors: StateKnot contributors
- Created: 2026-09-14
- Supersedes: none; additive to [RFC-0006](0006-authenticated-tool-result-reconciliation.md)

## Summary and motivation

An exact Unknown write attempt may subsequently have an authoritative failed
outcome with a known effect. The success-only operations endpoint cannot record
that evidence. Add `McpToolErrorReconciler`, not a generic remote Worker executor
or an expansion of the existing success Tool's authority.

## Goals, user-facing design and non-goals

Register `stateknot_reconcile_tool_error_v1` with the independent
`stateknot:reconcile-error` scope and mandatory `McpErrorReconciliationAuthorizer`.
The closed `McpErrorReconciliationRequest` carries original target/revision/digest,
stable event and failure IDs, bounded public failure category/code/message, and
`McpKnownToolEffect::{NotApplied, Applied}`. The complete host wiring and compiled
HTTP/PostgreSQL fixture are linked in the [English](../mcp-error-reconciliation.md)
and [Chinese](../mcp-error-reconciliation.zh-CN.md) runbooks.

Partial/unknown effects cannot become Failed through this profile. NotApplied
does not mean execution never started, no provider charge, or retry is safe.
Applied does not mean compensation occurred. Neither variant settles usage,
closes a Run, imports artifacts, accepts private diagnostic details, grants
remote SQL authority, or proves external exactly-once behavior.

## Detailed semantics

1. Verify the independent scope and current exact resource/effect policy before
   every database lookup, including duplicate recovery. The policy must verify
   authoritative original-operation evidence and approve the public message.
2. Construct the failure with host origin `mcp.reconciliation`, execution phase
   and `RetryAdvice::Never`. Reject ambiguous-external-outcome categories, even
   when a caller bypasses the MCP input-schema layer.
3. Hash RFC 8785 canonical `{tool, subject, tenant, principal, request}`. The tool
   name domain-separates errors; the success-v1 digest formula stays unchanged.
4. Verify the exact historical Unknown revision, digest, attempt and original
   frozen descriptor. Construct error provenance locally and apply core error
   validation; the caller cannot select schema, phase, origin or retry policy.
5. Recover only the identical immediate next revision: event ID, error audit
   kind/schema, request digest, predecessor and Failed status must all match.
   Otherwise claim (never supersede) a new host lease and recheck the receipt.
6. Commit `ReconcileError` and the authorization audit in one existing fenced
   transaction. A success/error race has one winner; the other profile conflicts
   even with the same event ID. At most four journal-CAS retries; no dispatch.
7. Release only the owned fence. The handler's 15-second deadline includes policy,
   database operations and release. Cancellation, timeout and disconnect do not
   prove rollback. Identical receipt recovery needs no lease and remains subject
   to current permission. Retry boundedly with the same complete request.

The record becomes Failed, not Committed or Unknown. Downstream execution must
handle that retained failure under existing execution/accounting rules; this
endpoint does not authorize a new Tool attempt or invent a zero-cost outcome.

## Persistence, compatibility and rollback

Reuse schema-24 immutable Tool revisions and journal audit, without migration or
new dependencies/MSRV. New audit kind `mcp-tool-error-reconciled` uses schema
`https://stknot.com/schemas/runtime/mcp-tool-error-reconciliation/1.0.0` with
version 1.0.0 and its RFC 8785 digest. Startup requires exact registered bytes.
It records trusted principal/policy and request/policy/decision digests, not raw
failure text. The existing ledger stores the public failure message.

Success-v1 name, scope, input/output/audit schemas and request-digest algorithm
remain unchanged. Golden schema digests and success-path database tests guard
compatibility. Both profiles share only private fenced persistence machinery.
Keep original schemas, policy/evidence artifacts and ledger history for the
entire unresolved/receipt recovery window. Roll back by disabling new admission;
retain a reader capable of the new audit schema for already accepted receipts.
Do not delete records or downgrade audit interpretation to success-v1.

## Security, privacy and operations

Use a dedicated HTTPS operations audience, issuer-namespaced identity, short-lived
credentials and separately granted scopes. Never publish this Tool to ordinary
Agent/Worker catalogs. Scope and syntactic validation are not effect evidence:
the application-owned authorizer must check immutable provider operation IDs,
target/attempt mapping and the business definition of Applied versus NotApplied.
Refuse uncertain or partially applied writes. No allow-all implementation ships.

Failure messages are bounded to 1,024 UTF-8 bytes with control characters refused;
this does not make arbitrary text non-secret. The evidence policy approves its
public projection; protect ledger backups. Debug output and MCP errors exclude
failure bodies and raw SQL errors. Do not log request bodies or credentials.
Bound ingress size, rate, concurrency and DB pool use; deployment-specific load,
failover, RPO/RTO and evidence-verifier reliability still require qualification.
Use fixed error-code counters and authorized audit lookup, not high-cardinality
payload labels. Alert on sustained Busy/Unavailable or unresolved-evidence age.

No atomic transaction spans the external evidence system, policy and database;
the host and database owner remain trusted. Recheck policy on every submission;
revocation blocks historical receipts, but cannot retract an earlier commit.

## Alternatives considered

- General ToolError JSON import: grants remote retry/provenance/details authority
  and admits uncertainty; exceeds this verified use case.
- Add failure fields to success-v1: silently widens scope and breaks its closed
  request/digest boundary. Use independent names and policy instead.
- Manual SQL repair: bypasses immutable transitions, fencing and audit. Existing
  trusted in-process reconciliation remains available without remote ingress.

## Validation and rollout

Mandatory PostgreSQL 16/17 HTTP qualification covers both known effects, denied
and cross-tenant access before lookup, scope separation, frozen provenance,
Never retry, same-event conflicts, held leases, lost HTTP delivery after commit,
24 duplicate reads per effect, fresh service recovery under a later live lease,
stale-fence rejection, 24-way first-error and mixed success/error submission races.
Schema startup drift, closed wire shapes, redacted Debug and success-v1 golden
schemas are additional gates. CI requires all three machine-readable evidence
markers and retains source/tree/lock/environment artifacts for 30 days.

Roll out opt-in on a restricted operations endpoint after domain evidence-policy
review, not by granting all result operators the new scope. Retain pre-alpha
status. Tests use isolated fixture identities and provider evidence, not a live
business provider; they prove ledger/protocol invariants, not external effects.

## Unresolved release questions

Domain evidence-verifier qualification, arbitrary effectful Worker isolation,
artifact/partial-effect handling, provider settlement/fencing and full
process/failover/capacity acceptance remain separate release gates.
