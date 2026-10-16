<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0025: MCP and A2A identity, authority and recovery mapping

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-10-15
- Tracking: [R1 identity and protocol security mapping](../r1-contract-gap-ledger.zh-CN.md), [PR 161](https://github.com/StateKnot/StateKnot/pull/161)
- Supersedes: None
- Superseded by: None
- Review baseline: b12ac21c0946787b2f0c3e0f06b64c590219f200
- Implementation status: records existing restricted Interfaces and proposed host requirements; no new verifier, durable backend or production qualification
- [中文对照](0025-protocol-identity-and-authority.zh-CN.md)

## Summary

Define the trust mapping between local durable execution and MCP/A2A peers.
Authentication identifies a caller; local policy separately authorizes a
particular operation and resource. A remote routing tenant, descriptor pin,
Agent Card, correlation ID or successful discovery grants no local authority.
Inbound credentials are not forwarded as outbound credentials. Recovery needs
its own current authorization and the original frozen durable identity.

This Draft is independent of RFC-0004's child-Run contract and RFC-0024's
proposed stdio owner. It does not turn the existing policy traits into a bundled
identity provider or the A2A conformance fixture into a durable task backend.
Acceptance requires the concrete identity/policy composition and evidence below.

## Fixed source observations

| Surface | Fixed baseline contract | Host responsibility |
| --- | --- | --- |
| MCP HTTP ingress | `McpServerAuthenticator` returns a bounded `McpServerPrincipal` with subject and scopes; decoded Tool/Resource/Prompt policies are separate | Verify the credential, retain issuer provenance and bind the subject to a local principal/tenant and exact operation |
| A2A ingress | `A2aServerPrincipal` carries a `TenantId`, complete `PrincipalIdentity` and scopes; `A2aServerAuthorizer` runs before backend lookup | Verify identity and the trusted tenant binding; implement resource policy, shared admission and durable `A2aTaskService` |
| MCP durable HTTP egress | `connect_authorized` pins the complete raw descriptor and calls `McpToolAuthorizer` after the durable attempt start | Bind discovery credentials to this configured endpoint; authorize actual input, trusted Run identity and destination on every new attempt |
| A2A egress | The immutable client pins discovery/interface/security; the token provider receives operation, card digest, binding, remote tenant/task, required scopes and optional local attempt tuple | Bind the provider to the exact configured endpoint and resolve current principal/consent from host-owned provenance |
| MCP stdio | RFC-0024 is Draft; no session Implementation exists | Select and prove the OS owner and frozen local process security scope before publishing Tools |

These are observations at the fixed baseline, not a statement that arbitrary
custom trait implementations satisfy this RFC. Existing API compatibility paths
and direct protocol clients remain distinguishable from durable dispatch.

## Identity and authority model

Use the exact verified `(issuer, subject)` as the external identity. Display
names, email, an Agent Card name, Tool metadata and model output cannot replace
that pair. A trusted control plane maps it to one explicit local tenant and
effective principal with a finite policy lifetime. Effective permission is the
intersection of verified credential authority, local binding and resource policy.
An absent, expired, ambiguous or unavailable binding grants no permission.

At the fixed review baseline, MCP's principal has no issuer or tenant fields. A multi-issuer host
must resolve verified issuer/subject to a bounded, opaque, host-owned principal
key before constructing it, and preserve the full mapping outside the protocol
subject string. The key must not alias another issuer/subject during any cursor,
request-state or audit lifetime. Rebinding the same external issuer/subject to
another local tenant, effective principal or binding generation also requires
a different key, or proof that the original mapping is immutable throughout
every associated state lifetime. Current private cursors bind subject/scopes;
MRTR associated data binds subject/Tool/arguments without another local tenant
field. Reusing the key cannot be described as revoking that old state. Do not concatenate unescaped identity strings
or treat an arbitrary subject as `TenantId`. Freeze and test the concrete key
and lifecycle representation before acceptance; the candidate below now authors
concrete types whose numeric-corrected qualification is recorded below.
A2A already carries the complete identity and explicit local tenant, which must
come from the authenticator's trusted mapping.

A local Run ID, invocation ID, attempt ID, worker fence and journal head are
correlation and durable ownership facts, not remote login credentials. A trusted
`ToolContext` correlates dispatch but does not by itself prove an end user's
identity, consent or current capability. Resolve those from host-owned records.
Delegation requires a separately supported verifier/policy that retains actor,
effective principal, intended resource and narrowing; unsupported delegation or
sender-constrained credentials must be refused, never silently downgraded.

## Inbound mapping

Configure exact public authorities, optional browser Origins, protocol mounts,
credential verifier, allowed issuer/audience/resource, expiry and revocation
policy before opening admission. Use verified TLS at the external edge and a
trusted proxy configuration; untrusted forwarding headers cannot choose these
values. Verifier configuration is operator-owned, not selected from a token URL.
Existing Agent HTTP JWT/JWKS/introspection profiles do not automatically verify
MCP or A2A tokens: their audience, scopes, context and authorizers are distinct.

Keep the current protocol-specific order. MCP authenticates and performs shared
admission before the decoded handler applies scope/operation policy and argument
validation. A2A authenticates before body decoding, authorizes the bounded decoded
operation before task/config lookup, then obtains shared admission and invokes
the backend. Transport `Mcp-Method`/`Mcp-Name` hints cannot replace decoded facts.
Do not claim that their admission order or every rejection status is identical.

MCP scope-filtered discovery, private cursor identity binding and authorization
before existence/schema diagnostics remain mandatory. A2A's backend receives
`A2aRequestContext` and must use its principal tenant for every storage key.
Request tenant overrides cannot alter that storage tenant. Opaque task/context/
push-config IDs are untrusted lookup inputs, never an ownership proof.

The ordinary public A2A Agent Card is discovery data, not permission to invoke
or fetch an extended card. MCP anonymous loopback and memory/loopback conformance
fixtures are explicit development profiles, not public deployments. Do not
install an allow-all policy to adapt one of them into production.

## Outbound mapping and data release

Freeze a deployment binding before dispatch: exact destination and transport,
reviewed descriptor/card evidence, remote name/skill, local schemas, approved
extensions and security alternative, remote routing tenant and credential
provider. Discovery drift requires a new reviewed binding. A descriptor pin
authenticates the reviewed description; TLS authenticates the configured server
under its trust roots. Neither proves executable behavior or remote exactly-once
effects. Embedded Agent Card signatures remain data without a configured verifier.

The A2A remote tenant comes from the selected interface and is a remote routing
identifier. It is independent of the local storage tenant. Never copy it into
local storage authority or derive it automatically from the local tenant.
Same text in those two namespaces does not establish a relationship.

Issue a destination/audience/resource-scoped credential for the exact approved
operation. MCP's approved per-call request exposes the exact endpoint and input.
Its startup callback receives only the local descriptor, so its provider must
already be bound to the reviewed endpoint and discovery authority. At that baseline, the
A2A authorization request includes card digest and binding but no endpoint
or actual payload accessor; its provider must be dedicated to the frozen endpoint or obtain that
binding from trusted construction, never infer an endpoint from a card digest
alone. Shared-provider endpoint disambiguation is an explicit open Interface
decision, not an already implemented check.

Do not relay an inbound bearer token, database credential, worker lease,
control-plane credential or upstream provider secret to a peer as ambient
authority. Approved arguments/message parts are an explicit data release: the
Adapter cannot prevent a trusted application from deliberately putting a secret
there. Local schemas validate shape, not permission to disclose the values.
That baseline A2A callback cannot authorize actual arguments from an attempt
tuple or a shape schema alone. Before acceptance, fix and execute an auditable
host pre-send payload policy/wrapper or review an Interface exposing the same
actual destination/request payload. Its check must cover initial send, streaming
send and attested MessageIdReplay, with zero business send on refusal. MCP
approved requests already carry actual input; these capabilities must not be
misrepresented as equivalent existing checks.

For durable execution, commit the exact physical attempt start before policy
and possible business send. Current MCP approved calls authorize validated
input under the remaining deadline; A2A providers receive the local attempt
tuple on the durable path. Direct `McpClient`/`A2aClient` use has no durable
dispatch authority and cannot replace this sequence inside a recoverable Graph.
The existing MCP `connect` compatibility path does not acquire the approved
path's input-aware policy or connection-retirement guarantee by this RFC.

## Revocation, state and uncertain outcomes

Recheck current policy and credentials on each new outbound operation and each
recovery request. Bound token and local policy lifetime. A prior permit, queued
request, discovered capability or cached schema is not a revocation bypass.
Revocation cannot roll back a request already sent or bytes already buffered;
the receiving resource must enforce its own credential/resource policy.
Long-lived streams and continuations need an explicit expiry/recheck contract
and bounded lifetime; do not silently inherit Agent HTTP's SSE recheck cadence.

MRTR state remains bound to the authenticated principal, operation and configured
state codec/keyring. Sampling, Roots and Elicitation are untrusted requests for
host action, never implicit access to files, models or UI. Host policy approves
each response; a continuation cannot migrate identities or acquire broader
authority. RFC-0024 separately specifies stdio generation and credential scope.

Preserve the actual effect mappings rather than claiming a common protocol
error means no write. For write-risk, MCP HTTP transport loss, incomplete/isError
outcomes and A2A ambiguous post-send outcomes retain their existing Unknown
handling. MCP ReadOnly instead uses NotApplicable: protocol loss has
SafeAfter(250ms), while incomplete/isError use Never; it is not an uncertain write.
The current MCP HTTP malformed-complete-result write path instead records
`Applied + Never`; RFC-0024 proposes `Unknown + ReconcileFirst` for stdio.
This Draft records that difference and changes neither path. Acceptance must
explicitly review it; a shared helper cannot silently select another mapping.

MCP result/known-error reconciliation requires separately authorized evidence
bound to the original endpoint, descriptor and durable attempt; it does not
resend the write. A2A recovery is separately configured: Disabled performs no
provider I/O; ContextTaskHistory only probes bounded exact original history;
durable artifacts use the pinned exact task handle; MessageIdReplay can resend
only under the explicit durable-deduplication attestation and matching delivery
profile. An Agent Card or repeated message ID is not that attestation.
Current reconciliation authority does not come from the credential that was
valid when the original attempt ran. Unknown remains Unknown when the host
cannot authenticate complete authoritative evidence.

## Durable storage, audit and operations

No token, grant, live client, request context or process credential is a durable
execution capability. Persist only the existing explicit identity/provenance,
descriptor/schema pins, attempt/effect/result and audit contracts. A serializable
`PrincipalIdentity` is an audit value, not proof that its holder is authenticated.
Do not serialize an arbitrary policy object or replay a stale authorization
decision as a current grant. This Draft itself applies no database migration. The explicit state profile
candidate below invalidates old private cursors/continuations; unconfigured
behavior stays legacy.

A2A server deployments still owe durable idempotency, tenant-scoped projections,
stable cursors, committed streams/cancellation and encrypted push secrets with
SSRF-aware egress and a transactional outbox. The protocol router and official
TCK fixture do not supply them. Capacity and cross-replica admission need real
deployment measurements. A2A/MCP server-to-server routing does not establish
Worker-only database isolation; that remains a separate deployment contract.

Audit bounded local decision/evidence identifiers, exact reviewed binding and
policy generation, operation, disposition and recovery result. Restrict access
to identity-bearing audit data and define its retention. Ordinary metrics/logs
must omit credentials, argument/result bodies, request state and remote error
text. Existing MCP raw-response Debug derives are not already payload-free;
RFC-0024's proposed Debug change needs actual implementation and regression.
Readiness must include the deployed verifier, fresh tenant/resource policy,
shared quota and durable backend; a successful discovery probe is insufficient.

## Selected A2A request-facts Interface

The additive experimental Interface extends `A2aClientAuthorizationRequest`
with `request_method() -> &str`, `request_url() -> &str` and
`request_body() -> Option<&[u8]>`. This addresses the baseline fact-visibility
gap described above. It adds no provider trait, dependency, verifier or implicit
allow/deny decision; acceptance of the complete identity profile remains open.

Both unary and streaming paths prepare the original bounded request before
credential resolution. The same private immutable request supplies the policy
facts and is consumed by the HTTP builder. Existing Bytes storage shares the
serialized body without another full payload copy; no public Bytes type is
required. HTTP+JSON and JSON-RPC retain their original different envelopes,
request IDs, URLs, query encoding, headers, limits, deadlines and dispatch/effect
classification. These accessors do not expose a universal header policy object.
The deployment still binds its configured headers/extensions and credential
scope. Anonymous skips the provider; existing/static providers that ignore new
facts retain their previous behavior.

The host can inspect actual data and refuse before dispatch, including a new
current-policy check on recovery. This does not prove that every custom provider
checks arguments or trusted Run consent. MessageIdReplay keeps the original
business message identity, while its JSON-RPC envelope gets a fresh correlation
ID; evaluate the actual new round. A callback still pending at deadline or drop
must be dropped without sending, and synchronous work remains bounded by the
host. Nothing grants an atomic transaction between remote effects and policy.

Manual Debug omits the newly visible URL/body, while existing tenant/task/attempt
fields remain visible. Raw accessors are intentional data release to trusted
host policy. Cloning shares and can retain the payload past the HTTP call;
host-owned retention is explicit. The concrete validation must compare all
prepared method/URL/query/body facts against real wire requests, exercise
payload/destination refusal, pending callback deadline/drop, legacy/anonymous
behavior and currently authorized attested recovery. PostgreSQL evidence must
observe the committed original attempt inside policy and prove refusal causes
no business send. Duplicate recovery of the same committed durable Failed or
Unknown attempt receipt must add neither another send nor a second policy
evaluation. A fresh probe or attested MessageIdReplay remains a new operation:
it re-applies current policy and may send under its original recovery contract.

## Selected experimental MCP host-bound state candidate

The selected numeric-corrected
source is `69af3a0f…` (928 files; 536 compiled inputs `ded1a866…`). Six local gates
pass: 99 workspace summaries/2,098 passed/6 original ignored, 48 doctests,
strict Rust1.88 Clippy/Rustdoc and fixed nightly, original opt0/default stack.
A fresh default-feature opt0 binary `0751e63128bf…` runs all15 cases serially on
PostgreSQL16.15/17.11, each15/0/0/0 and one complete marker. The13TCP/1constructor/
1DB inventory includes only three consumers/six commits/twelve refused rebinds/
two full fixture Run/Journal ledgers, not all tables or a general backend.
See the [bounded record](../qualification/experimental-mcp-lossless-bound-state-2026-10-16.md)
and [server guide](../mcp-server.md#experimental-host-bound-state-candidate).
This is not RFC acceptance, complete CI, Supported deployment or a published API.

The typed binding retains stable deployment namespace, verified external
issuer/subject, local tenant, effective issuer/subject and non-reused positive
u64 generation. One fingerprint includes that full tuple and canonical scopes.
Syntax is not provenance verification. Current host policy/admission checks the
full mapping after queueing; its successful check is the authorization
linearization point. No dynamic backend or zero-gap in-flight revocation is
added. Generation cannot repeat or wrap, including ABA and revoke/reactivate.

Explicit immutable configuration remains on the composite builder and four
direct services. The ten owned context-bearing RPC entries, including empty,
unmounted and actual Skill internal dispatch paths, enforce host binding.
Synchronous static metadata and SDK control methods gain no business authority.
Private catalog caching does not change explicit Public Resource contents or
replace the existing HTTP authenticator/verifier.

Strict private cursors remain v2. All three strict operation AD domains are v3;
private envelopes remain integer1 with exactly required version/applicationState
keys, null allowed but missing refused. Tool/Prompt argument encoding clones the
actual bounded parsed Value, recursively sorts object keys and serializes with
serde_json::to_vec: sorted serde_json value encoding, not JCS or original wire
spelling. It preserves wide integers, integer/float and signed-zero
representations. Resource has no argument part. The real numeric counterexample
is Tool-only; public Prompt still validates declared strings before AD. Numeric
helper tests do not widen that schema. Valid same-key old v2 states refuse on
all three domains without fallback; legacy unconfigured v1/JCS remains unchanged.

Decoded policy sees untrusted wire state; after original permit/schema checks
and successful guard opening, only the business request's state changes to the
inner application value. Any present decoded responses, even an empty map,
requires bound state. Complete envelope64KiB/wire128KiB/positive TTL<=24h remain.
Output wrapping is after business, so failure grants no zero-effect/retry
inference. Types remain non-Serde with scoped redacted Debug; raw getters and
Clone retain existing sensitive-data responsibilities.

Historical eedd six-gate/14-case successes remain, but the actual wide-integer
TCP counterexample prevents adopting that old exact-argument candidate. Original
strict-gate/fixture/marker failures and new E0597/socket failures stay preserved.
Fresh v3 gates do not transfer old Native/916 executions or qualify arbitrary
host implementations. The Markdown outline is not a separately compiled external
consumer. Preceding85f CI ended11success/2original30m cancellations despite both
Store264/Runtime189 terminal greens; it contains no MCP candidate. Expanded
Ubuntu/test-opt1 MCP and complete CI, deployed mapping/verifier/delegation,
long streams, A2A durable backend, stdio and R1 remain open.

## Validation and acceptance

1. Freeze concrete issuer/subject-to-local-principal/tenant mapping, MCP key
   representation, audience/resource/scopes, policy leases and revocation rules.
   Demonstrate same-subject/different-issuer refusal, rejection of old cursor/MRTR
   state after the same external pair/scopes changes local tenant/effective
   principal, and unchanged storage tenant under malicious routing/body/metadata
   fields through public Interfaces.
2. Exercise every mounted operation's authentication/policy/lookup order,
   existence hiding, cursor/MRTR identity substitution, scope narrowing and
   unavailable/expired policy. Record zero application or storage dispatch on
   applicable refusal paths, including queued revocation and startup drift.
3. Capture real outbound requests and prove exact destination, remote routing
   tenant, per-attempt context and scoped credential choice; no ambient token,
   durable fence or database credential may appear on the wire. Test provider
   reuse across otherwise similar cards/endpoints and actual payload release
   policy through unary, SSE and attested MessageIdReplay. Attempt correlation
   alone grants no argument authority.
4. Force durable-start-before-send, lost acknowledgement, deadline/Future drop,
   credential revocation and process replacement. Unknown writes must not be
   repeated except the explicitly attested A2A MessageIdReplay profile. Bind all
   recovery probes/evidence to the original attempt and current authorization.
5. Qualify actual verifier/secret-manager and remote deployments over verified
   TLS, real database roles, cross-replica admission and restore/rotation. Test
   the attested remote retention/dedup/history window; loopback/TCK success does
   not satisfy this deployment evidence. Keep exact source/binaries/markers.
6. Review existing MCP HTTP invalid-result mapping, concrete A2A endpoint-aware
   provider Interface, long-lived stream revocation and durable A2A task/push
   backend. Complete full workspace/protocol/Native/release gates plus the
   independent security review required by the release milestone.

Existing guides and tests are starting evidence, not completion of this matrix.
R1 and this RFC remain open until their actual scoped acceptance conditions are
met. Acceptance cannot certify arbitrary user-supplied policy implementations.

## Alternatives and unresolved decisions

Reusing a protocol tenant, subject string, Tool annotation or card scope as
local authority would remove the independent host policy. Forwarding inbound
tokens would make unrelated resources share an audience/consent assumption.
Generic automatic resend would discard the durable Unknown contract. None is
selected. A single universal principal or transport trait is unnecessary to
describe the real existing Interfaces and would hide their current differences.

The concrete MCP host principal key is frozen and authored in the selected
candidate section, with bounded local component acceptance above. Deployed
current input-aware A2A policy
using the selected request facts, verifier/delegation profiles, stream revocation
behavior and durable A2A backend remain unresolved. Any resulting public types, dependencies
or durable semantics require review under the RFC process before support claims.

## Source map and claim boundary

The baseline observations use the fixed local source; the selected A2A request
facts are the additive experimental change described separately. External protocol
conformance and a general authentication standard are not newly claimed here.
The future stdio session is only the RFC-0024 proposal. This text alone adds no
production policy, tests, credentials or platform capability.

- [MCP ingress](../../crates/stateknot-integrations/src/mcp_server.rs)
- [MCP application policies](../../crates/stateknot-integrations/src/mcp_server_app.rs)
- [MCP approved egress](../../crates/stateknot-integrations/src/mcp_authorization.rs)
- [MCP effect mapping](../../crates/stateknot-integrations/src/mcp.rs)
- [A2A ingress and task contract](../../crates/stateknot-integrations/src/a2a_server.rs)
- [A2A client identity and discovery](../../crates/stateknot-integrations/src/a2a_client.rs)
- [A2A durable delivery/recovery](../../crates/stateknot-integrations/src/a2a_remote_agent.rs)
- [Explicit Core identity](../../crates/stateknot-core/src/identity.rs)
- [MCP authorization guide](../mcp-call-authorization.md)
- [A2A client guide](../a2a-client.md)
- [A2A server guide](../a2a-server.md)
- [Draft stdio owner](0024-owned-bounded-mcp-stdio.md)
- [RFC process](README.md)
