<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0006: Authenticated Tool result reconciliation

- Status: Draft (implemented qualification profile; not release acceptance)
- Authors: StateKnot contributors
- Created: 2026-09-13
- Supersedes: none

## Motivation and scope

A durable remote write can retain `Unknown` after its response is lost. Running
the write again is unsafe; an authorized operations service needs to record an
authoritative result without receiving SQL credentials or a Run fence. Reuse
the existing MCP 2026-07-28 server rather than creating a parallel HTTP stack.

This profile handles only inline successful Tool results for an exact original
Unknown attempt. It is not general Worker execution, error/artifact import,
provider settlement or a claim of exactly-once external effects. The
[English](../mcp-reconciliation.md) and [Chinese](../mcp-reconciliation.zh-CN.md)
guides are the host integration and incident runbook.

## API and identity

`McpToolReconciler` implements the existing `McpServerToolHandler`. Register its
closed `definition()` on an authenticated, privileged operations endpoint.
`McpReconciliationRequest` contains an event ID, target Run/invocation/original
attempt, exact Unknown revision/digest and output. It carries no caller tenant,
principal, fence, schema, policy, artifact or error authority.

The authenticator supplies a verified issuer-namespaced subject and the explicit
`stateknot:reconcile-result` scope. A mandatory `McpReconciliationAuthorizer`
maps it to a trusted `AgentServiceCaller` and verifies exact resource permission
and result evidence before database lookup, including for duplicate receipts.
Its grant retains policy identity, policy digest and decision/evidence digest.
There is no allow-all authorizer. Scope, JSON shape and Worker self-assertion
cannot substitute for external evidence. Deployers implement their business
provider's evidence verification and immutable evidence retention.

## Observable transaction and retry semantics

1. Authorize; bind the canonical request digest to subject and mapped tenant/
   principal. The MCP exchange ID is not a durable idempotency key.
2. Load the exact historical Tool revision with its base checkpoint, checksums,
   direct predecessor transition and journal/projection anchor verified in a
   repeatable-read snapshot. Reject any target, attempt, digest or status drift.
3. Build result provenance from the frozen invocation, not remote arguments;
   validate output against its pinned offline schema and Tool limits.
4. Recover an identical next-revision receipt when already committed. Match
   event ID, audit kind/schema/request digest, predecessor and Committed state.
   Different output, subject or event ID conflicts. Policy changes do not change
   an existing receipt, but current permission is always required to read it.
5. Otherwise claim a fresh host lease without superseding an owner. A live Worker
   gets priority; return Busy. Recheck the receipt after claiming the lease.
6. Commit `ReconcileResult` and its authorization audit atomically using existing
   PostgreSQL Run fencing, journal CAS and projection-bound event identity. Only
   bounded journal-head retries are allowed; no provider dispatch occurs.
7. Release only the claimed fence. Lost release acknowledgement cannot undo a
   durable receipt. Timeout/drop can leave a lease until database-clock expiry;
   identical committed receipt recovery requires no lease and changes no state.

The handler has a 15-second total deadline including policy and DB work; the
existing HTTP service bounds bodies and concurrency. Cancellation and timeout
do not prove rollback. Retain the same logical request and event ID and retry
with bounded backoff. Stored records, not an HTTP status, determine the outcome.

## Persistence, compatibility and integrity

No migration or new table: schema 24's immutable revision and journal tables are
the receipt store. `load_tool_invocation_revision` is an additive indexed exact
lookup; it validates a direct predecessor, not arbitrary full history. A row
missing beneath the current pointer is corruption, not a fresh operation.
Existing trusted runtime grants suffice; never issue them to a remote Worker.
The audit schema is pinned at startup to its canonical bytes. Keep old schema
versions and evidence as long as unresolved invocations/receipts are retained.

Only existing workspace dependency edges change (serde and test reqwest). No
package version, MSRV, existing wire schema or existing reconciler behavior is
changed. The versioned MCP Tool and audit schema provide an explicit boundary
for future incompatible semantics. This remains a Draft contract exposed by the
exact public-preview release, not an accepted stable contract.

## Security and operations

Require verified HTTPS and audience-scoped credentials, trusted issuer mapping,
current revocation and resource/evidence policy. Never expose this Tool in the
ordinary Agent/Worker catalog or reuse its credentials for provider execution.
Put DB access only in the trusted host. Bound infrastructure request rates,
connection counts, pool capacity and private evidence retention. Do not log
credentials, request bodies, output or raw SQL errors. The journal stores
principal/policy attribution and digests; the result is in the existing ledger.

No distributed transaction exists between the authorizer, provider and database.
Evidence must describe the exact immutable original operation; permission can
change after a decision. Check policy on every submission and enforce scoped
short-lived credentials at the receiving resource. A host authorizer or DB owner
is trusted; this does not protect against either being compromised.

## Alternatives and qualification

Raw SQL grants to operators/Workers bypass the intended service boundary.
Replaying an Unknown write confuses availability with effect safety. Accepting
arbitrary result/error/usage claims expands authority beyond this bounded need.
An independent custom RPC transport duplicates existing protocol enforcement.

Real authenticated HTTP and PostgreSQL 16/17 tests cover auth before parsing,
scope refusal, denied existing/missing targets, policy before DB lookup (even
with the pool closed), tenant/attempt/digest/schema refusal, active-lease Busy,
atomic audit, client disconnect after commit before response delivery, 24-way
first submission and duplicate receipt races, conflicting identities/results,
fresh-service recovery under a different live lease and stale-fence rejection.
CI retains source/tree/lock/environment evidence. These are not production
capacity, arbitrary in-transaction fault, failover or external-effect proofs.

The RFC remains Draft pending broader protocol/security/operations acceptance.
The general effectful Worker API, error/artifact evidence profiles, executable
attestation and provider-specific external fencing remain separate gates.
