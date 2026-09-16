<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0011: Online Agent HTTP authentication and expiring tenant bindings

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-09-16
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/54
- Supersedes: None
- Superseded by: None

## Summary and motivation

Provide a concrete OAuth 2.0 RFC 7662 introspection authenticator for the
existing mandatory HTTP authentication hook. A finite, default-deny local
binding policy maps exact verified issuer/subject identities to tenants and
intersects access-token operation scopes with host grants. This profile is
usable behind verified TLS without inventing a new authorization server.
It remains pre-alpha, not acceptance of a stable security API.

## Goals and non-goals

Implement bounded HTTPS verification, client-secret rotation, online revocation
observation, finite tenant-policy freshness and dependency readiness. Do not
implement JWT/JWKS verification, discovery, login/refresh flows, DPoP/mTLS-bound
tokens, token caching, arbitrary policy languages, a secret manager, or run-owner
ACL storage. The mandatory AgentService resource authorizer remains independent:
a tenant binding is not permission to read every Run in that tenant.

## User-facing design

`AgentHttpIntrospection` implements `AgentHttpAuthenticator` and
`AgentHttpReadiness`. Construction takes explicit endpoint/issuer/audience/client
configuration, a bounded redacted client secret, and an `AgentHttpTenantPolicy`.
Custom trusted CA certificates may be supplied; insecure TLS is never an option.
The host installs the same shared instance in HTTP authentication and composes
its readiness check with the real resource-policy dependencies.

Tenant bindings explicitly contain tenant, principal and permitted operation
classes. Policy creation and replacement require a finite lease. Replacement
uses an expected generation, rejects duplicates and validates before publication;
failed replacement leaves the previous snapshot intact. An empty snapshot denies
all callers. No token/body tenant claim is trusted. Client-secret replacement is
atomic; requests already started may finish with the previous credential.

## Detailed semantics

Only a fixed HTTPS URL without userinfo/query/fragment is permitted. Disable
redirects, environment proxies, HTTP retries and content decompression. Bound
connections, total deadline, concurrency without waiting, response bytes and
JSON shape. Client authentication follows RFC 6749 client_secret_basic including
form encoding before Basic encoding. Token, secret and provider response text
never appear in errors or Debug. Zeroizing wrappers cover owned secret buffers,
not copies made inside HTTP/TLS libraries or the allocator.

Active responses must contain exact configured `iss`, matching string/array
`aud`, valid `sub`, integer future `exp`, integer `iat` not in the future and
strictly before expiration, and Bearer `token_type`. Total token lifetime is
bounded to one hour by default, configurable up to 24 hours in whole seconds.
Optional `nbf` must already be
valid. No positive clock-skew allowance extends token lifetime. Reject `cnf`
because this profile cannot verify sender constraints. The provider must limit
accepted Bearer token types to access tokens for this resource; the request's
access-token hint is not a security guarantee. Scope syntax and counts are
bounded. Missing optional RFC claims required by this stricter profile deny
authentication; providers must be configured accordingly.

Every request and SSE revalidation calls introspection; no cached active result.
Unknown/inactive/wrong claims return sanitized unauthenticated, whereas provider
failure, malformed protocol responses, exhausted capacity, poisoned locks,
expired policy or deadline return unavailable. Authentication takes one current
policy snapshot after remote verification. Replacements affect subsequent
checks, not already authorized operations. SSE retains its bounded revalidation
and queued-event exposure window; revocation is not instantaneous rollback.

## Readiness and operations

Check policy freshness before and after a negative introspection canary using
current client credentials. A fresh random non-token must return `active:false`.
This proves current TLS/client-authenticated endpoint access, not successful
business login, authorization or future availability. No synthetic principal is
granted and no Agent operation runs. Compose with real resource-policy readiness;
never replace it with this probe. Use an independent canary permission or provider
allowlist if required, and qualify the provider's invalid-token behavior.

The monotonic policy lease is at most one hour, never automatically renewed;
the trusted control plane must revalidate and atomically publish fresh bindings.
Rebuild it after restart from the trusted source, not caller claims. Alert on
unavailable authentication/readiness, refresh failures and IdP capacity. Budget
at least one IdP request per HTTP request/SSE polling iteration plus probes;
replica-wide quotas and abuse protection belong at the proxy/IdP.

## Persistence, compatibility and rollback

No schema migration, persisted token or new durable policy evidence. Existing
resource-authorizer grants/evidence remain authoritative. Binding generations
are process-local CAS guards, not audit digests or distributed consistency.
Configuration/secret rollout and durable control-plane auditing remain host
responsibilities. Additive Rust 1.88 APIs use the already-locked Reqwest stack.
Rollback selects the previous qualified application/configuration together;
never enable anonymous ingress to restore availability.

## Security and privacy

Only host-controlled endpoints and CA roots receive credentials. Egress policy,
DNS trust and endpoint ownership remain deployment controls. No credential-
derived network destinations, fallback identities, wildcard subjects or default
grants. Scope permission and resource policy are both required. Separate
introspection client credentials from caller credentials and use a secret
manager for delivery. No test realm, accounts or Agent API is deployed publicly.

## Alternatives considered

Mandatory traits alone leave every host reimplementing a sensitive verifier.
JWT/JWKS verification removes online availability dependence but requires a
separate qualified cache/rotation/revocation profile. Online introspection is an
explicit supported tradeoff, not a claim to implement all OIDC mechanisms.

## Validation and rollout

Test strict configuration and claims, duplicate/deep/oversized JSON, TLS trust,
redirects, authentication outages, secret rotation, revocation, deadline and
concurrency release, policy CAS/expiry/revocation and cross-tenant isolation.
Qualify a pinned isolated Keycloak with verified TLS; retain real PostgreSQL
and owned-server/SSE regression gates. Update bilingual guides and publish exact
source-tree evidence before merge and website-only deployment.

## Unresolved questions

Independent security/contract review and broader provider qualification remain
release gates. No unresolved question permits weakening this implemented profile.

## References

- <https://www.rfc-editor.org/rfc/rfc7662>
- <https://www.rfc-editor.org/rfc/rfc6749#section-2.3.1>
- <https://www.rfc-editor.org/rfc/rfc9700>
- <https://www.keycloak.org/securing-apps/oidc-layers>
