<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent HTTP online identity profile

`stateknot::agent_http::introspection::AgentHttpIntrospection` is a concrete
OAuth 2.0 token introspection verifier, qualified against Keycloak 26.7.3 over
verified TLS. It implements `AgentHttpAuthenticator` and `AgentHttpReadiness`.
This is a bounded pre-alpha integration profile, not full OIDC, JWT/JWKS local
validation, a login service or a stable production release. [中文](agent-identity.zh-CN.md)

## Trust and authorization

Configure the HTTPS introspection endpoint, exact issuer, resource audience,
confidential client ID and three distinct operation scopes. The provider must
authenticate the introspection client and report access-token status for this
resource. `token_type_hint=access_token` is only a hint, not proof of token type.
Do not configure an endpoint that reports refresh/ID tokens as Bearer access
tokens for the same audience. Sender-bound `cnf` tokens are rejected.

Use `ClientSecret::new(delivered_secret)` with a value delivered by your secret
manager. Use `IntrospectionOptions::with_root_certificate` for an explicit
private CA; public system roots remain enabled. No insecure TLS switch exists.
URL userinfo/query/fragment, redirects, environment proxies, retries, content
decompression, discovery and token-selected endpoints are disabled.

Active responses must contain exact `iss`, matching string/array `aud`, valid
`sub`, integer `iat` and `exp`, and Bearer `token_type`. Expiration must be in
the future, issuance not in the future, and total lifetime within the configured
limit. Optional `nbf` must already be valid. There is no expiry-extending clock
skew: synchronize host/IdP clocks. Scopes use RFC 6749 token syntax; duplicate,
empty or excessive scopes are rejected. This is deliberately stricter than
RFC 7662's optional-claim baseline; configure provider mappers accordingly.

`TenantBinding::new(tenant, principal, operations)` comes from a trusted control
plane, never an unverified claim, body or forwarding header. The exact verified
issuer/subject maps to one tenant. Effective operation permission is the
intersection of token scopes and local binding grants. Unknown principals are
denied; empty policies grant nothing. No wildcard or anonymous identity exists.

**The existing `AgentServiceAuthorizer` remains mandatory.** It still decides
exact Agent/request and Run/submission access before database lookup, supplying
the existing durable policy evidence. A tenant binding does not confer tenant-
wide read/cancel access or implement ownership ACLs. This module does not replace
that authorizer or make its dependency health checks optional.

## Wiring and fresh policy

See the compiled Rustdoc example for constructing the verifier. Pass its shared
`Arc` to `AgentHttpService::new(service, verifier, options)`. Retain the same
`Arc<TenantPolicy>` for trusted refresh and the verifier for credential updates.

`TenantPolicy::new(bindings, lease)` starts generation 1. Before the finite
monotonic lease expires, revalidate the trusted source and call
`replace(expected_generation, fresh_bindings, lease)`. It returns the next
generation; stale generation, duplicates or invalid limits leave the old
snapshot unchanged. Do not blindly renew a cached decision after losing the
source. Empty replacement explicitly revokes all bindings. An expired or
poisoned policy makes authentication unavailable. Restart must reload from the
trusted source; generations are local CAS guards, not distributed/audit versions.

Changes apply at subsequent verification checks, not retrospectively to
already authorized operations. Distribute changes and audit them through the
host control plane. No background refresh thread or secret persistence is hidden
inside the verifier.

## Readiness, rotation and failure handling

Compose readiness with the actual resource-policy dependency:

```rust
use std::sync::Arc;
use stateknot::{agent_http::{AgentHttpReadiness, AgentHttpReadinessError,
    introspection::AgentHttpIntrospection}, core::BoxFuture};

struct Dependencies {
    identity: Arc<AgentHttpIntrospection>,
    resources: Arc<dyn AgentHttpReadiness>,
}
impl AgentHttpReadiness for Dependencies {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentHttpReadinessError>> {
        Box::pin(async move {
            self.identity.check().await?;
            self.resources.check().await
        })
    }
}
```

The identity probe checks policy freshness, sends a fresh random invalid-token
canary using current client authentication, requires `active:false`, then checks
freshness again. It grants no synthetic principal and performs no Agent operation.
It proves endpoint/TLS/client-credential access, **not** successful business
login, resource permission, provider correctness or future availability. Qualify
this negative-token behavior with your provider. The owned server also checks
the actual database schema and executable registry under its whole-check deadline.

Rotate the provider credential, deliver the replacement through the secret
manager, then call `replace_client_secret`. Existing requests may finish with
the previous secret. No retry or old-secret fallback occurs. Without an IdP
overlap window, rotation intentionally creates a brief fail-closed 503 window;
coordinate rolling delivery to avoid it. Qualification rotates the real IdP
secret, observes readiness loss, installs the new secret and verifies recovery.
Trust-root/endpoint/client-ID changes require constructing a new verifier and
performing the normal staged host rollout.

Inactive/expired/wrong-claim credentials return the existing sanitized 401;
valid identity without operation or resource permission returns 403. Provider
401/5xx, malformed/oversized responses, timeout, exhausted verifier capacity or
expired local policy return sanitized 503. Never log raw tokens, client secrets,
provider bodies or HTTP Authorization headers. Debug is redacted for owned
secrets; zeroization cannot cover every HTTP/TLS/allocator copy.

## Capacity and revocation

| Boundary | Default / maximum |
| --- | --- |
| Total remote exchange deadline | 3 s / 10 s |
| Concurrent remote exchanges, no queue | 32 / 256 |
| Response body | 64 KiB fixed |
| JSON | Depth 8, 128 entries/container, 1024 nodes |
| Token lifetime | 1 h / 24 h; whole seconds |
| Scope list | 64 tokens, 128 bytes/token |
| Tenant snapshot | 1024 exact principal bindings |
| Policy lease | Explicit nonzero duration, at most 1 h |

There is no active-token cache. Each HTTP request and SSE authorization cycle
uses online introspection. SSE defaults to a one-second polling cycle and has
its existing queue/lifetime limits: already buffered bytes cannot be revoked.
No guarantee stronger than the provider's revocation visibility is claimed.
Budget IdP load for all streams, HTTP traffic, replicas and probes; enforce
replica-wide quotas at the proxy/IdP. Monitor unavailable responses, dependency
freshness and secret/policy refresh failures through protected host telemetry.
When uncertain, refuse new work rather than grant a fallback identity.

## Reproducible qualification and deployment

`conformance/agent-identity/run.sh` starts one loopback-only, digest-pinned,
resource-limited Keycloak with a newly generated test CA and leaf certificate.
It requires a dedicated real PostgreSQL URL. No password grant is used; fixture
service accounts use client credentials. Fixed fixture secrets are test-only.
The script removes its exact container, anonymous volume and generated keys on
exit; it never touches unrelated services. CI runs it with PostgreSQL 16 and 17
and requires exactly one `STATEKNOT_IDENTITY_EVIDENCE` marker.

Tests prove trusted/untrusted TLS, real submit/read, resource-policy denial,
cross-tenant denial, operation narrowing, real client-secret rotation/readiness
recovery, real subject disable/revocation, SSE closure, policy expiry and joined
HTTP drain. Unit tests additionally cover hostile JSON/headers, claims, CAS,
timeouts and cancellation capacity release. This is interoperability evidence,
not blanket RFC/Keycloak certification or the complete production failure matrix.

Deploy only after separately qualifying your IdP configuration, secret delivery,
policy source, external resource policy, TLS proxy, SQL roles, Worker/scheduler
lifecycle and monitoring. The fixture realm/server/accounts are never a
production identity deployment. No schema migration or public API deployment
is required to publish these website guides. [RFC-0011](rfcs/0011-agent-http-introspection.md)
remains Draft pending independent security and stable-contract acceptance.

References: [RFC 7662](https://www.rfc-editor.org/rfc/rfc7662),
[RFC 6749 client authentication](https://www.rfc-editor.org/rfc/rfc6749#section-2.3.1),
[Keycloak OIDC endpoints](https://www.keycloak.org/securing-apps/oidc-layers).
