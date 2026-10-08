<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0018: Local JWT access-token verification with operator-managed JWKS

- Status: Accepted
- Authors: StateKnot contributors
- Created: 2026-10-01
- Supersedes: None; complements RFC-0011

## Motivation and scope

Provide a concrete local verifier for the existing Agent HTTP authentication
and readiness hooks. A trusted control plane associates an issuer with a
bounded, recently verified JWKS snapshot. Authentication remains available
without a per-request IdP exchange while that snapshot and the independent
tenant policy remain fresh.

This additive source-tree profile implements signed RFC 9068 access tokens
using RS256. It does not accept ID tokens, encrypted JWTs, sender-constrained
tokens, shared-secret signatures, arbitrary algorithm negotiation, token-selected
network destinations, discovery, login or refresh-token issuance.
Per-token online revocation continues to use RFC-0011 introspection.

## Public API and trust boundary

`AgentHttpJwtJwks` implements `AgentHttpAuthenticator` and
`AgentHttpReadiness`. `JwtJwksOptions` fixes issuer, resource audience, three
distinct operation scopes, optional inspection scope and finite capacity.
Construction receives verified public JWKS bytes, a freshness lease and the
existing shared `TenantPolicy`; `TenantBinding` and `TenantPolicy` are
re-exported without changing their published error type or semantics.
The resource authorizer remains mandatory.

Only the trusted host can call `replace_jwks(expected_generation, bytes, lease)`.
All keys are validated before one atomic CAS publication; failed parsing or a
stale generation leaves the previous set intact. A deliberately empty replacement
revokes all keys and removes readiness. Generations are process-local guards,
not durable audit or issuer versions.

The operator must obtain the JWKS from its configured issuer using authenticated
delivery, verify the issuer-to-key association and freshness, and audit rollout
to every replica. Delivery may be verified HTTPS or a signed configuration
channel. Never renew an old cached set after losing the source. Account for the
age of retrieval when choosing the remaining lease. Rebuild from trusted
configuration after restart; no caller-controlled key selection performs I/O.

## Exact verification rules

Require compact, unpadded Base64url JWS with three nonempty segments. Strict
bounded JSON rejects duplicates and excessive shape before verification.
The header permits only `alg`, `typ` and `kid`: RS256, case-insensitive
`at+jwt` or `application/at+jwt`, and one exact nonempty key ID.
No embedded keys, URL headers or critical extensions are accepted.

JWKS is at most 16 KiB and 16 unique keys. Every admitted key is an RSA
public signing key explicitly marked `alg=RS256` and `use=sig`; optional
`key_ops` must be exactly `["verify"]`. RSA modulus is 2048–4096 bits with
exponent 65537. Reject known private-key members, weak/malformed material and
duplicate key IDs. Public certificate metadata has no authority over the
verified `n/e` key material and is never fetched.

Reuse pinned `aws-lc-rs` 1.18.1, already present in the TLS dependency graph,
for parsed RSA public keys and RS256 signature verification. JWT JSON remains
on the existing duplicate-rejecting bounded parser; no process-global crypto
provider selection or additional JWT parser participates in authentication.
The workspace MSRV remains Rust 1.88.
Validate exact issuer, matching bounded string/array audience, valid subject,
integer `exp/iat`, optional integer `nbf`, nonempty bounded `client_id/jti`
and bounded distinct OAuth scopes. Reject `cnf`; no positive clock-skew
allowance extends validity. Token lifetime defaults to 15 minutes and is at
most one hour. Tenant claims never select a tenant. Effective operation grants
intersect verified scopes with trusted bindings before separate resource policy.

## Lifetime, concurrency and failures

Signature work runs off the async executor in a bounded blocking-task pool:
32 admitted jobs by default, at most 64, with no admission queue. The permit
remains owned by the job after caller cancellation or timeout. A timed-out
queued job is aborted; already running bounded verification may finish.
The total verification deadline defaults to three seconds and is at most ten.
These are per-verifier limits; replica-wide traffic limits remain host controls.

After verification, recheck token time and the selected keyset generation and
freshness before resolving the current tenant policy. A concurrent key update
or lease expiry returns unavailable. Subsequent HTTP and SSE checks use the new
set. Already authorized operations and buffered SSE bytes are not rolled back.
Short token lifetime bounds ordinary JWT revocation delay; `jti` presence
does not create a revocation ledger.

Invalid token, signature, claims or unknown key gives sanitized 401; scope or
resource denial gives 403. Expired/poisoned snapshots, exhausted verification
capacity, deadline or blocking-task failure give 503. Never surface token,
claim, key or library error text. Owned credentials remain redacted/zeroized;
JSON/crypto-library copies are outside that wrapper's ownership.

Readiness checks fresh nonempty keys and fresh tenant policy. It is local
dependency evidence; compose it with actual schema/executable/store and
resource-policy readiness. It does not probe IdP availability or verify that
the control plane fetched the correct issuer keys.

## Compatibility, validation and rollout

No database migration or change to durable admission evidence is required.
Existing introspection users retain their APIs. Rollback switches application
and trusted identity configuration together.

Unit tests use two real RSA keys and prove signature/tamper checks, ID-token
and algorithm confusion rejection, exact claims, hostile/duplicate JSON,
private/weak key refusal, bounded capacity, freshness, rotation and one-winner
CAS replacement. Real HTTP/SSE with PostgreSQL 16/17 proves resource/tenant
denial, key removal, event-stream closure, expiry/recovery, joined drain and
fresh-verifier exact-key admission replay. CI retains the JWT qualification
marker with existing Agent HTTP evidence.

## Maintainer contract review — 2026-10-08

Accepted for this additive source profile. The review traced HTTP authentication,
current tenant resolution, independent resource policy, SSE revalidation,
blocking-job ownership and key publication. Existing introspection error types,
durable admission identity and database schema remain unchanged.

The draft's RustCrypto RSA/JWT dependency chain failed the repository's advisory
policy. The accepted implementation reuses AWS-LC for key validation and
signature verification, and the existing bounded JSON parser for all JWT claims.
No advisory exception or process-global crypto provider was introduced.
Real issuer qualification uses a separate Keycloak client configured with
`access.token.header.type.rfc9068=true`, RS256, an explicit `client_id` mapper
and the resource audience. The TLS-pinned host filters that issuer's JWKS to the
configured signing profile before provisioning it.

Acceptance authorizes the declared contract; implementation and deployment
qualification are separate. R0 retains exact regression/CI/deployment evidence.
Independent final-release security assessment, authenticated multi-replica key
refresh and production-topology qualification remain later release gates. Trusted
key-delivery integration, other algorithms/issuers and full production-topology
qualification require their own evidence; the whole framework release gates
remain controlling.

## References

- [RFC 9068](https://www.rfc-editor.org/rfc/rfc9068)
- [RFC 8725](https://www.rfc-editor.org/rfc/rfc8725)
- [RFC 7517](https://www.rfc-editor.org/rfc/rfc7517)
- [aws-lc-rs 1.18.1](https://docs.rs/aws-lc-rs/1.18.1/aws_lc_rs/signature/index.html)
