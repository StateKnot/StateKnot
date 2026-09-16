<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0012: Bounded declarative Agent resource policy

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-09-16
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/57
- Supersedes: None
- Superseded by: None

## Summary and motivation

Implement `runtime::agent_policy::AgentResourcePolicy` as a concrete, offline,
default-deny `AgentServiceAuthorizer`. Online identity verification and tenant
mapping do not grant access to resources. Hosts need a usable bounded policy
implementation, not an allow-all example. This experimental profile remains
pre-alpha; passing its tests does not accept a stable security contract.

## User-facing design and scope

`PolicyDocument` contains format version 1, an exact policy capability identity,
an absolute `valid_until`, submission rules and run rules. `PolicyArtifact::new`
validates and canonicalizes trusted configuration. `from_json` additionally
requires an independently trusted expected digest and rejects duplicate keys,
unknown fields and excessive structure. Retain `canonical_bytes()` in immutable,
access-controlled deployment storage before installing the artifact. A checksum
is integrity binding, not authentication or signature verification.

Each submission rule matches exact tenant, issuer/subject, Agent revision and
input schema and grants explicit scopes plus a nonempty restrictive budget.
Input content is not a policy language: normal offline schema validation still
runs at admission. Run rules name Read or Cancel and either an exact Run ID,
tenant-bound submission-key digest, or explicitly privileged `TenantRuns` target.
`TenantRuns` grants all runs and key lookups in that one tenant; it is never
inferred from identity or submission permission. No wildcards, implicit ownership,
rule priority, inheritance, external PDP, login flow or durable ACL service.

## Detailed semantics

At most 1024 total rules and 256 KiB bounded JSON. Reject duplicate submission
selectors and overlapping run selectors for the same caller and operation.
No match denies before deployment/database existence lookup. Empty policies are
valid and deny all. Authorization takes one immutable snapshot; replacement does
not roll back an already authorized request. SSE uses its existing repeated
authorization and bounded buffered-event exposure window.

Artifact SHA-256 binds the entire normalized canonical document. The selected
rule checksum separately binds the policy identity, rule type, exact selector,
scopes and budgets with a versioned digest domain. This selected-rule checksum
is the grant's `policy_digest`: unrelated rule changes and lease refresh cannot
break lost-ack submission recovery. Changing the applicable submission rule
intentionally changes admission authority and may return submission conflict;
recover the old result through an independently authorized key lookup instead
of bypassing current policy or silently reusing an obsolete grant.

Submission evidence binds that rule checksum and a domain-separated canonical
checksum of the complete request. A pinned closed offline JSON Schema validates
the admission evidence. The budget layer uses the same deterministic evidence
digest. No current timestamp, random nonce or local generation enters these
digests. Run decision checksums bind rule checksum, caller, operation and exact
target; an operator grant never loses the actual target in its evidence.

`AgentResourcePolicy::new` and `replace` require a finite monotonic freshness
lease of at most one hour. The artifact's absolute expiration also bounds the
monotonic deadline at installation, so a later backwards clock adjustment cannot
extend that installation. Reopening an expired artifact fails. Replacement uses
expected-generation CAS and validates before publication; failures preserve the
previous snapshot. Generation is local concurrency control, not audit identity
or cross-replica consistency. No automatic renewal or fallback policy.

## Persistence, migration and compatibility

No database migration or dependency change. Admission already persists authority,
schema-bound evidence and budget restrictions. Cancellation already persists
policy/decision checksums; read and denial decisions are not journaled. This
profile does not introduce a durable access-decision ledger. Operators must
retain the protected policy artifacts and access audit context required by their
retention policy; cancellation hashes alone cannot reconstruct the caller.
Artifacts may contain sensitive resource identifiers and must not be public.
The framework does not claim tamper resistance against its trusted control plane.

Rust 1.88 APIs are additive. Register the new pinned evidence schema before
freezing the executable registry; retain that schema while referenced admissions
exist. Roll back application and matching retained configuration together;
never bypass resource authorization to restore availability.

## Operations and security

`check_readiness` checks the current resource policy without granting a synthetic
operation. The facade provides `AgentHttpReadiness` integration; compose it with
real identity readiness in a host using introspection. Expired/poisoned policy
is unavailable; an unmatched caller is denied. Errors contain no configuration,
request, credential or provider details. Refresh only after validating the trusted
source, monitor failed refreshes/expiration, and synchronize the host clock.
Replica-wide distribution, durable auditing, privileged configuration access and
Worker/scheduler lifecycle remain explicit independent host responsibilities.

## Alternatives and validation

Traits alone require every host to build this boundary. General policy languages
and network PDPs expand the operational and retained-decision contract; neither
is required for explicit static/service-account ACLs. Automatically granting run
ownership would require a separately qualified durable ownership model.

Validate configuration limits, duplicate/overlap denial, all identity/resource
dimensions, deterministic request/rule/target evidence, restrictive budgets,
artifact integrity, CAS races, absolute and monotonic expiration, readiness and
sanitized diagnostics. Real PostgreSQL 16/17 HTTP tests must prove idempotent
recovery across unrelated policy refresh, refusal of cross-tenant and unlisted
targets, durable policy evidence, cancellation, policy-revoked SSE cleanup and
no inline execution. Retain machine-readable CI evidence and bilingual guides;
deploy only the documentation website, not test identities or a public Agent API.

## Unresolved acceptance

Independent stable-contract review, complete identity/policy-backed deployment
qualification and independently managed Worker/scheduler roles remain open.
