<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent resource authorization

[中文](agent-resource-policy.zh-CN.md) · [RFC-0012](rfcs/0012-agent-resource-policy.md)

`stateknot::runtime::agent_policy::AgentResourcePolicy` is a concrete offline,
default-deny `AgentServiceAuthorizer` for explicitly provisioned service-account
and resource ACLs. It is separate from OAuth verification and tenant mapping.
This implemented profile is pre-alpha, not a stable API or full production release.

## Configure and retain

Build a `PolicyDocument` in the trusted control plane. Its fields are
`format_version: 1`, `policy: CapabilityIdentity`, `valid_until: Timestamp`,
`submissions: Vec<SubmissionRule>` and `runs: Vec<RunRule>`. Do not accept this
document, its expected digest or its tenant/principal bindings from an HTTP caller.

- A `SubmissionRule` contains `tenant`, `principal`, `agent`, `input_schema`,
  `granted_scopes`, and nonempty `budget_limits`. Every selector is exact, including
  issuer, subject, Agent owner/version and schema digest. The policy permits input
  conforming to that schema; it does not interpret business data as ABAC predicates.
  All budget dimensions must still resolve to finite values across the policy,
  immutable Agent and request layers. A request can only tighten them.
- A `RunRule` contains `tenant`, `principal`, `operation: RunPermission` and
  `target: RunAccessTarget`. Read and Cancel are separate grants. `Run(id)` does
  not grant key lookup; `Submission(key.digest_for(&tenant))` grants only that
  lookup. Cancellation is by Run ID, never by key.
- `TenantRuns` is an **explicit privileged operator permission** over all runs
  and key lookups in the named tenant. Never use it as a substitute for ownership.
  A submission grant alone gives neither read nor cancel permission.

`PolicyArtifact::new(document)` rejects ambiguous selectors, unsupported versions,
empty budget layers and more than 1024 total rules. Default bounded-JSON limits
also apply: 256 KiB, depth 32, 1024 entries/container, 16384 nodes, 64 KiB/string
and 256 bytes/key. Empty rules are valid deny-all configuration. Unsupported
cancellation-by-submission targets are rejected instead of silently doing nothing.

Persist `artifact.canonical_bytes()` in private immutable deployment storage and
publish `artifact.digest()` through an independently authenticated manifest before
activation. Keep old artifacts for your run/audit retention window. The digest is
SHA-256 over the normalized canonical typed document, not raw file formatting;
it is not a signature. A loader must bound file/download bytes before allocation
and pass a trusted expected digest to `PolicyArtifact::from_json`.

## Wire the actual service

The following loader is also compiled as a runtime documentation test:

```rust
use std::{sync::Arc, time::Duration};
use stateknot::core::Digest;
use stateknot::runtime::{JsonSchemaRegistryBuilder, agent_policy::*};

fn load_policy(
    retained_bytes: &[u8], trusted_digest: Digest,
    schemas: &mut JsonSchemaRegistryBuilder,
) -> Result<Arc<AgentResourcePolicy>, PolicyError> {
    register_agent_policy_evidence_schema(schemas)?;
    let artifact = PolicyArtifact::from_json(retained_bytes, trusted_digest)?;
    Ok(Arc::new(AgentResourcePolicy::new(artifact, Duration::from_secs(300))?))
}
```

Register the evidence schema **before** freezing your executable registry, along
with the existing graph/admission/service-control schemas. Pass the same shared
policy to `AgentServiceV1::new(store, executable, deployments, policy.clone())`.
The HTTP facade implements `AgentHttpReadiness` for this policy. When using online
identity, compose resource readiness before and after
`AgentHttpIntrospection::check().await`; do not replace one dependency with the
other. `AgentHttpServer` additionally verifies actual storage and executables.
The real Keycloak test exercises this composition, not an allow-all policy stub.

## Refresh, revoke and recover

Use `generation()` as an expected-generation CAS guard for
`replace(expected, fresh_artifact, lease)`. Validate the trusted source again;
never automatically renew cached authority. Lease must be positive and at most
one hour. Absolute artifact expiration is retained across restart and also caps
the monotonic lease, preventing a backwards clock adjustment from extending an
installed snapshot. Synchronize the host clock. Invalid refresh or a stale CAS
leaves the old snapshot unchanged. No cross-replica consistency is implied.

No matching rule returns denied (HTTP 403). Expired/poisoned policy returns
unavailable (503), never a cached grant. Replacements affect later checks, not
already authorized commits. SSE rechecks resource policy and closes on revocation;
bytes already queued to the client cannot be retracted. Empty fresh policy is
healthy and denies everyone; readiness does not mean any user is authorized.

The whole artifact digest identifies a rollout. A grant's `policy_digest` instead
binds the **selected rule plus policy identity**. Request evidence includes a
domain-separated digest of the complete schema/input/budget request. Consequently,
refreshing expiry or another resource's rules preserves lost-response submission
recovery. Changing the selected submission rule changes authority: retrying its
old key may return 409. Recover through an independently authorized key lookup;
do not bypass policy or resubmit under a new key automatically.

## Evidence and operating limits

Admission stores the authority, pinned closed evidence schema, request/rule
checksums and restrictive budget layer in the existing atomic durable record.
Cancellation stores existing policy/decision checksums, not a new full decision
ledger. Read and denied decisions are not journaled. Retain protected artifacts
and access audit context as required; cancellation hashes alone cannot identify
the caller. Never log Bearer credentials, raw submission keys or request bodies.
Artifact configuration can contain sensitive identity/resource identifiers.

This is not automatic run-owner ACL storage, a generic policy language, an
external PDP, a policy signature verifier or a managed host. Provision exact Run
and key grants explicitly, or deliberately install a privileged tenant operator.
Monitor freshness and rejected refreshes; coordinate policy rollout across
replicas. Retain the new schema for all admissions that reference it and roll
back matching application/configuration together. There is no database migration
or dependency change. Worker/scheduler role management remains separate.

## Executable qualification

`cargo test -p stateknot-runtime --lib agent_service::policy --locked` covers
selectors, bounded/closed artifacts, overlaps, deterministic evidence, schema,
expiry, poisoned state and concurrent CAS. Real PostgreSQL 16/17 HTTP tests require
`STATEKNOT_REQUIRE_POSTGRES_TESTS=1` plus a dedicated `STATEKNOT_TEST_DATABASE_URL`.
They withhold a committed HTTP response, refresh unrelated grants, recover the
same run, inspect stored evidence, cancel idempotently, revoke a live SSE and
reject expired policy without invoking a node. CI requires exactly one
`STATEKNOT_RESOURCE_POLICY_EVIDENCE` marker. The separate pinned Keycloak profile
also uses this concrete resource policy. Test accounts are never deployed publicly.
