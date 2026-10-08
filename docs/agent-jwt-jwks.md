<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent HTTP local JWT/JWKS identity

`stateknot::agent_http::jwt_jwks::AgentHttpJwtJwks` verifies RFC 9068
access tokens locally with an operator-provisioned public JWKS. It implements
`AgentHttpAuthenticator` and `AgentHttpReadiness`. This is an additive
source-tree profile; the published `0.1.0-alpha.1` preview does not contain it.
[中文](agent-jwt-jwks.zh-CN.md)

## Configure and install

Use the compiled Rustdoc example to construct `JwtJwksOptions` with one exact
issuer, resource audience and distinct submit/read/cancel scopes. Supply
verified public JWKS bytes and a remaining freshness lease to
`AgentHttpJwtJwks::new(options, jwks, lease, policy)`.

`TenantPolicy` and `TenantBinding` are shared with the online identity profile
and re-exported from `jwt_jwks`. Trusted bindings map an exact issuer/subject
to one tenant; verified scopes only narrow their operation permissions.
Token tenant claims have no effect. The separate `AgentServiceAuthorizer`
must still authorize each exact Agent, Run and submission key before lookup.
Pass a shared verifier `Arc` to `AgentHttpService::new` and compose its
readiness with actual store/executable/resource-policy readiness.

The authorization server must emit signed `at+jwt` access tokens for this
resource. Required claims are `iss`, `sub`, `aud`, integer `iat/exp`,
nonempty `client_id/jti`, and operation scopes when permissions are needed.
Optional `nbf` must already be valid. Exact issuer/audience, bounded total
lifetime and zero expiry-extending clock skew are enforced.
ID tokens (`typ=JWT`), `cnf` sender-constrained tokens, encrypted tokens,
HMAC/other algorithms and JOSE header extensions are rejected.

## Key delivery and rotation

Obtain keys from the configured issuer over verified HTTPS or an independently
authenticated configuration channel. Verify issuer association and retrieval
freshness before provisioning. The verifier never contacts URLs from tokens
or key metadata. A public JWKS is not secret, but changing it changes which
identities can authenticate; protect delivery and audit every update.

Every key must carry a unique bounded `kid`, `kty=RSA`, `alg=RS256` and
`use=sig`, with a 2048–4096-bit modulus and exponent 65537. Optional
`key_ops` is exactly `["verify"]`. Private fields and malformed/weak keys
are rejected. At most 16 keys and 16 KiB are accepted. Unsupported keys must
be filtered by the trusted provisioning process into the reviewed RS256 set.

Refresh using `replace_jwks(expected_generation, verified_jwks, remaining_lease)`.
Parsing and validation complete before atomic publication. Invalid input or
stale generation preserves the old set. Use `generation()` for the local CAS
guard. Publish an overlap set during planned rotation, move issuance to the
new key, then remove the old key after the accepted token window.
An empty replacement deliberately revokes all keys and removes readiness.

Renew only after revalidating the source. Subtract time since retrieval from
the lease; refreshing old bytes is not evidence of freshness. The key lease
and tenant policy lease are independent, each at most one hour. Rebuild both
from the trusted source after restart and distribute revocations to every
replica. No hidden background task or credential persistence is installed.

## Failure, SSE and capacity

Invalid signatures/claims or removed/unknown keys return the existing sanitized
401. Missing operation/resource permission returns 403. Expired snapshots,
verification saturation, poisoned locks or verification timeout return 503.
After cryptography, token time and key generation/freshness are checked again
before current tenant-policy resolution.

SSE revalidates with the same verifier on each polling cycle. Removing a key
closes streams that use it; already buffered bytes and previously authorized
operations cannot be revoked retroactively. JWT has no per-token online
revocation here. Use a short token lifetime, explicit tenant revocation, or the
[online introspection profile](agent-identity.md) when immediate provider
revocation visibility is required. Required `jti` is not a replay denylist.

| Boundary | Default / maximum |
| --- | --- |
| Accepted algorithm/type | RS256; `at+jwt` or `application/at+jwt` |
| Bearer credential | 8192 bytes |
| Signature jobs per verifier | 32 / 64; no admission queue |
| Verification deadline | 3 s / 10 s |
| Token lifetime | 15 min / 1 h; integer seconds |
| JWKS | 16 KiB; 16 unique public keys |
| Key lease | Explicit nonzero duration, at most 1 h |
| Trusted bindings | 1024; independent lease at most 1 h |

RSA verification runs in Tokio blocking tasks. An admitted job retains its
capacity permit until completion, including after caller cancellation or
deadline. Replica-wide limits, clock synchronization and authenticated key
delivery belong to host deployment. Local readiness proves fresh keys and
policy, not current IdP availability or correctness of the operator's key source.
Never log bearer headers, token bodies or raw crypto-library errors.

## Qualification

Unit tests generate two real test-only RSA keys in memory and exercise signature
tampering, algorithm/ID-token confusion, exact mandatory claims, duplicate JSON,
unsafe keysets, CAS races, expiry and capacity recovery. The real PostgreSQL
16/17 Agent HTTP matrix verifies independent resource and cross-tenant denial,
key removal, SSE closure, key-expiry recovery, joined shutdown and exact-key
admission recovery through a fresh verifier/listener.
The evidence marker is `STATEKNOT_JWT_JWKS_EVIDENCE`.


The real TLS Keycloak 26.7.3 gate additionally verifies issuer-produced RFC 9068
access tokens, trusted filtered JWKS provisioning, tenant/scope mapping, durable
same-key admission replay and empty-key revocation. Its separate fixture client
sets `access.token.header.type.rfc9068=true`, RS256, a `client_id` claim mapper
and the explicit resource audience. This validates that configuration, not
arbitrary OIDC providers. See `conformance/agent-identity/realm.json`.

No schema migration or production identity service is deployed with the website.
Qualify your issuer's access-token profile, trusted key distribution and refresh,
resource policy and multi-role deployment before exposing an application.
[RFC-0018](rfcs/0018-agent-http-jwt-jwks.md) accepts this scoped contract; full framework
production qualification continues under the [roadmap](roadmap.md).
