<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP server profile

> Status: implemented pre-alpha profile; public API is not stable.<br>
> Protocol revision: `2026-07-28` only.<br>
> Transport: stateless Streamable HTTP with complete JSON responses and
> request-scoped SSE.<br>
> Explicit exclusion: the MCP Tasks extension is not implemented or claimed.

StateKnot now has a StateKnot-owned MCP server application layer for Tools,
Resources, Resource Templates, Prompts, Completion, and multi-round tool,
resource, or prompt requests (MRTR). It can also compose the separate
[MCP Skills server profile](mcp-skills-server.md). The official Rust SDK remains
a private wire adapter; its domain types are not part of StateKnot's public API.

The server is still pre-alpha. This implementation does not make the whole
framework production-ready, stabilize the Rust API, implement an OAuth
authorization server, or add the Tasks extension.

## Production boundary

One request crosses the following boundaries in order:

```text
Host/Origin/body checks
  -> Bearer authentication
  -> replica-wide admission policy
  -> decoded method and resource lookup
  -> scope and operation authorization
  -> schema/argument validation
  -> bound-state opening (when explicitly configured)
  -> application handler
  -> bounded and schema-validated result
```

The transport does not trust `Mcp-Method` or `Mcp-Name` as authorization facts.
They are hints until the JSON-RPC request has been decoded and checked. Anonymous
serving is rejected unless every configured host is literal loopback.

`McpServerHttpService` provides:

- exact `2026-07-28` version enforcement and stateless protocol metadata;
- explicit Host and Origin allowlists with no wildcard escape hatch;
- streaming request-body, concurrency, and request-deadline ceilings;
- Bearer authentication with redacted credentials and a fixed RFC 6750/9728
  challenge;
- a caller-owned admission interface for shared quotas and rate limits;
- cooperative shutdown and cancellation propagation;
- complete JSON plus request-scoped SSE, with legacy sessions and `initialize`
  disabled.

Production deployments must use Bearer authentication, TLS at the public edge,
and a cross-replica admission implementation. `anonymous_loopback()` exists only
for local development and hermetic conformance runs.

## Application surfaces

`McpServerApplicationBuilder` composes only configured surfaces and advertises
only their capabilities. After applicable admission/profile checks, calling an
absent surface returns method-not-found;
StateKnot does not advertise an empty placeholder capability.

### Tools

`McpServerToolRegistryBuilder` freezes the executable Tool registry at startup.
It validates portable names, JSON Schema 2020-12 documents, catalog and schema
byte ceilings, duplicates, stable ordering, and a canonical registry digest.
Validators compile offline; unresolved or network `$ref` values fail startup.

Each call is resource-bounded before policy code runs. Authorization then runs
before input-schema diagnostics, preventing a denied principal from probing a
private Tool's schema. The handler runs only after authorization and input
validation. Structured results are checked against the registered output schema
before leaving the process. Text, image, audio, embedded resource, resource
link, and future protocol content are bounded and validated.

Progress is available only when the caller supplied a progress token. Handler
cancellation is cooperative. Tool-level failures remain Tool results; transport,
policy, and handler failures remain protocol errors.

### Resources and templates

The immutable Resource catalog validates absolute URIs, structurally validates
URI templates, enforces catalog ceilings, and freezes a stable digest. Reads are
authorized before existence is disclosed. Text and binary contents have MIME,
Base64, item-count, and aggregate-byte checks. Every result carries an explicit
TTL and public/private cache scope.

### Skills extension

An optional immutable Skill catalog advertises the Final SEP-2640
`io.modelcontextprotocol/skills` extension and exposes `skills/list`,
`skills/get`, and exact manifest-bound file reads. Its claim remains separate
from the consuming [static client and Host profile](mcp-skills-host.md); see the
[server claim boundary](mcp-skills-server.md).

### Prompts and Completion

The Prompt catalog validates names, unique bounded arguments, required fields,
scopes, ordering, and a stable digest. Authorization precedes existence and
argument diagnostics. Rendered messages accept bounded StateKnot text, image,
audio, and embedded-resource content.

Completion is optional and therefore advertised only when a provider is
configured. The provider receives a bounded authenticated Prompt or Resource
Template reference, current argument, and string-only context. Results contain
at most 100 unique values with internally consistent pagination metadata. The
provider owns target-specific completion authorization.

## Scope-aware discovery and caching

The following describes the existing unconfigured profile; the experimental
bound-state candidate is described separately below.

Tool, Resource, Resource Template, and Prompt discovery filters entries by the
authenticated principal's exact scopes. Private cursors bind the catalog digest,
principal subject, canonical scope set, surface, and offset; a cursor cannot be
replayed across identities or a changed catalog. Startup rejects public caching
when any catalog entry is scope-restricted.

Annotations, descriptions, schemas, server instructions, client capabilities,
completion values, and transport headers are data, never authority.

## Multi-round requests and request state

Tools, Resources, and Prompts can return `input_required` for Elicitation,
Sampling, or Roots input. StateKnot validates request count, IDs, payload size,
client responses, and opaque request-state size.

`McpServerRequestStateCodec` seals application JSON with an explicit keyring,
expiry, and associated data. The caller binds state to the authenticated
principal and exact operation through the request context. Keys must be at least
32 bytes, can rotate, never appear in `Debug`, and have a maximum 24-hour TTL.
Invalid, expired, tampered, or cross-operation state collapses to one public-safe
error.

Do not place secrets in request state. Sealing provides integrity and binding;
the payload still belongs to the application retention and privacy policy.

## Experimental host-bound state candidate

The numeric-corrected
unreleased candidate is fixed to 928-file source `69af3a0f…` and 536 compilation
inputs `ded1a866…`. Its six local gates pass with Rust1.88, original opt0 and the
default stack; separately built dual-PostgreSQL qualification is bounded below.
The [new bounded record](qualification/experimental-mcp-lossless-bound-state-2026-10-16.md)
and [machine receipt](qualification/experimental-mcp-lossless-bound-state-2026-10-16.json)
bind the exact source, binary and fixed-scope execution.
[RFC-0025](rfcs/0025-protocol-identity-and-authority.md) stays Draft; complete CI,
trusted mapping/verifier deployment, stdio, A2A durable server and R1 remain open.
These types are not available in the published alpha.1 package.

`McpServerHostBinding` records a stable deployment namespace, verified external
`PrincipalIdentity`, local `TenantId`, effective `PrincipalIdentity` and
`NonZeroU64` mapping generation. `McpServerPrincipal::from_host_binding` derives
an opaque `hb1:` subject and canonicalizes scopes; its `host_binding()` getter
retains the complete tuple. Syntax validation is not credential verification.
The existing authenticator must supply a current trusted mapping. The host must
never reuse or wrap a generation, including A-to-B-to-A remapping and
revoke/reactivate; generation exhaustion refuses new mapping authority. Replicas
accepting the same state deliberately share the namespace and current mapping.

An explicit `McpServerBoundStateProfile` is attached through consuming
`with_bound_state(profile)` on `McpServerApplicationBuilder` or a direct Tool,
Resource, Prompt or Skill service. The builder installs the same profile at
final build, regardless of construction order. Configured clones and later
Completion configuration retain it; a pre-configuration clone remains legacy.
Repeated configuration fails. Public catalog options fail at construction,
without a silent downgrade. Private here covers catalog metadata and
continuation identity: an application's explicit Public `McpServerResourceResult`
content keeps its independent cache contract and receives no new confidentiality
qualification. A host-bound principal alone does not enable the profile.

The candidate guards ten owned handler entries on the composite and all four
direct services: `discover`, `list_tools`, `call_tool`, `on_custom_request`,
`list_resources`, `list_resource_templates`, `read_resource`, `list_prompts`,
`get_prompt`, and `complete`. This includes empty Skill templates, unsupported
custom methods and unmounted operations. Direct unmounted entries retain the
locked SDK's original response after the guard. Internal Skill custom/read
entry points also check the binding. Skill file reads still refuse MRTR
requestState/inputResponses. Synchronous `get_info`/`get_tool` are
static metadata, not authenticated tenant authority. SDK control methods such
as initialize/ping, tasks, notifications and subscriptions gain no new identity
or business support. The profile does not replace HTTP authentication or create
an identity provider, A2A durable backend or stdio session.

All strict private cursor surfaces keep version `v2`, the common complete-binding/
canonical-scope fingerprint and original catalog/surface/offset checks. Strict
operation AD uses `v3` for **all three** Tool/Resource/Prompt consumers; the private
envelope remains integer version `1`. A valid same-key old v2 operation-state
token is refused without fallback, as are legacy v1 cursors and unsealed state.
Tenant, effective principal, external identity, namespace, generation or scope
changes invalidate associated old state before business dispatch. Reload lists
from their first page and coordinate in-flight continuation draining. Refusal
does not authorize automatic business restart or safe retry. Unconfigured
services preserve legacy v1 cursors, AD/JCS and application-owned opaque state.

Tool/Prompt argument binding uses **sorted serde_json value encoding**: clone
the actual bounded parsed Map as Value::Object, recursively sort object keys,
then serialize with serde_json::to_vec. It preserves decoded wide u64/i64 values,
integer versus float and signed-zero representations, arrays and nested values;
object key order is intentionally normalized. This is not JCS and does not
preserve original JSON numeric spelling. Resource has no argument field.
The reproduced numeric alias affected actual public Tool consumption. Prompt
still accepts only declared bounded strings before AD construction; internal
Prompt helper numeric tests do not establish a reachable numeric Prompt case
or expand that schema.

For Tool/Resource/Prompt MRTR, decoded authorization sees the bounded, untrusted
**wire** envelope. After the original policy and argument/schema checks, the
framework opens state against the current fingerprint and exact decoded
operation/arguments or URI, and the business handler sees a distinct immutable
request containing only the original **inner** application state. Responses and
operation data are unchanged. Every outgoing `input_required` is sealed even
when inner state is absent. The private version-1 envelope requires exactly
`version` and `applicationState`; explicit null restores None, but a missing
field is invalid. Any present decoded `inputResponses`, including `{}`, requires
valid bound state. Wire null keeps the existing SDK Option decoding. Bad incoming
state reaches no business handler; output wrapping failure happens after a
handler and is not evidence of zero effect or a new retry permission.

Count the complete JSON envelope, including names, version and escaping, within
64 KiB; retain the original 128 KiB wire-state ceiling and positive TTL of at
most 24 hours. There is no truncation or increase: some formerly accepted large
opaque application strings cannot fit this envelope. The codec authenticates
without encryption or single-use redemption. New binding/profile and principal
types have no Serialize/Deserialize/JsonSchema implementation. Their Debug
omits identities, raw scopes, keys, state and fingerprint; raw getters and Clone
can still retain trusted identity or payload data. Unrelated request/result/
context Debug is not thereby payload-free, and state must not contain secrets.

Current authorization remains host-owned. Existing decoded business policies
(and Skill/Completion providers) check the full current tuple/generation after
queueing before returning a permit. Catalog paths without a decoded policy
require the existing admission implementation to recheck after its queue. That
successful current check is the respective authorization linearization point;
the state guard adds no lookup backend or mutex across application I/O. Changes
before that point refuse safely; already authorized/in-flight data and long
streams have no zero-gap instant revocation promise. An Allow policy alone does
not demonstrate the deployment requirement.

The following functions use the actual locally compiled public Interface.
This Markdown outline itself is not an independently compiled consumer or a
complete authenticator/deployment. Pass verified host facts to the first
function and a configured codec/builder to the second; no fixture key, universal
verifier or synthetic backend is supplied.
```rust,ignore
use std::{num::NonZeroU64, time::Duration};
use stateknot_core::{PrincipalIdentity, TenantId};
use stateknot_integrations::{
    McpServerApplicationBuilder, McpServerBoundStateProfile, McpServerHostBinding,
    McpServerPrincipal, McpServerRequestStateCodec,
};

// Call from the authenticator with already verified, current host mapping facts.
// Typed inputs alone do not verify credentials, consent, or mapping provenance.
pub fn mapped_principal(
    authenticated_identity: PrincipalIdentity,
    tenant: TenantId,
    effective_principal: PrincipalIdentity,
    generation: NonZeroU64,
    scopes: Vec<String>,
) -> Result<McpServerPrincipal, Box<dyn std::error::Error>> {
    let binding = McpServerHostBinding::new(
        "inventory-mcp-v1", authenticated_identity, tenant,
        effective_principal, generation,
    )?;
    Ok(McpServerPrincipal::from_host_binding(binding, scopes)?)
}

// Call once at startup with Private catalog options and the host's keyring.
pub fn enable_bound_state(
    builder: McpServerApplicationBuilder,
    codec: McpServerRequestStateCodec,
) -> Result<McpServerApplicationBuilder, Box<dyn std::error::Error>> {
    let profile = McpServerBoundStateProfile::new(codec, Duration::from_secs(300))?;
    Ok(builder.with_bound_state(profile)?)
}
```

### Scoped local acceptance

The fixed source's six final local gates pass: Rust1.88 formatting, strict
workspace all-target/all-feature Clippy, 99 workspace summaries totaling 2,098
passed/0 failed/6 original ignored, 48 independent documentation tests, strict
Rustdoc and fixed nightly-2026-10-07 check. Original opt0, debug assertions,
overflow checks and default stack remain. Conditional DB entries in workspace
are not Native evidence.

Separately, a freshly built **default-feature** opt0 binary `0751e63128bf…`
runs the complete suite with `--nocapture --test-threads=1` on actual
PostgreSQL16.15 and17.11: each15 passed/0 failed/0 ignored/0 filtered, with exactly
one complete marker. The inventory is13 real TCP cases, one constructor case and
one real-PostgreSQL case. That DB case retains three consumers, six fixture
commits, twelve refused rebinds and two complete fixture Run/Journal ledgers;
it is not all database tables or a general durable server backend.

The new TCP family observes the real business-decoded argument bytes for eight
numeric/nested Tool pairs. Same-value and fresh changed-value operations succeed;
reusing the original token with a changed parsed value requires HTTP400 and
unchanged business facts/count/completions. Nested object-key reordering succeeds.
Prompt strings round-trip and numeric arguments retain the original schema
refusal. Two new private unit tests cover the shared numeric encoding and valid
same-key old-v2 refusal in all three domains; they are not three separately
executed HTTP old-domain scenarios.

Historical `eeddc9f7…` has six-gate/2,095-pass/48-doctest and both full serial
14-case successes. A later real TCP counterexample nevertheless accepted the
same token for `9007199254740992` and `9007199254740993` and added a business call.
Those raw greens and failures remain historical; that candidate cannot be
adopted as exact decoded-argument binding. The record also retains earlier
strict-gate/fixture/serial-marker failures, the correction candidate's original
Clippy E0597 and the initial sandbox socket denial. Fresh fixed-source results
do not rewrite them or transfer historical 916 Native executions.

On preceding main head `85f3fd5`, [CI38752172054](https://github.com/StateKnot/StateKnot/actions/runs/38752172054)
finished with 11 successful jobs and two ordinary PostgreSQL jobs cancelled at
their original 30-minute limit. Store264 and Runtime189 reached successful
terminal summaries on both majors; the remaining full job chains did not finish.
That tree contains no new MCP candidate. Its incomplete CI does not qualify
this composition. The mandatory expanded MCP CI step on Ubuntu/test-opt1 and
complete GitHub CI remain unexecuted; local loopback component tests do not
qualify Linux deployment, capacity, release, Supported or R1.


## Construction outline

The complete executable example remains in crate tests. The production-shaped
construction sequence exposed by the exact `0.1.0-alpha.1` public preview is:

```rust,ignore
let options = McpServerApplicationOptions::new(
    "inventory-mcp",
    "1.0.0",
    100,
    Duration::from_secs(60),
    McpServerCacheScope::Private,
)?;

let app = McpServerApplicationBuilder::new(options)
    .with_tools(tool_registry, tool_authorization)?
    .with_resources(resource_catalog, resource_reader, resource_authorization)?
    .with_skills(skill_catalog, skill_authorization)?
    .with_prompts(prompt_catalog, prompt_renderer, prompt_authorization)?
    .with_completion_provider(completion_provider)?
    .build()?;

let service = McpServerHttpService::with_admission_control(
    app,
    http_options,
    McpServerAuthentication::bearer(authenticator, bearer_challenge),
    admission_control,
)?;
```

Mount `service` at one exact endpoint in Axum, Hyper, or another compatible
Tower host. Do not create registries per request. Build and validate every
definition before accepting traffic.

## Verification evidence

The frozen official runner is
`@modelcontextprotocol/conformance@0.2.0-alpha.11`, source revision
`c321dd32035556e6769d3724a8ee97d87c3faaac`, against requirement revision
`2026-07-28`.

The strict transport fixture passes all 37 scored Server scenarios exactly:
114 successful assertions, five explicit capability skips, one informational
SSE check, zero failures, and zero warnings. Three pending, unscored JSON Schema
and HTTP-header gates add 32 successful assertions with zero failures or
warnings. StateKnot's application surfaces separately pass real HTTP boundary
tests for capability discovery, pagination, authorization ordering, schema
validation, Tool dispatch, Resource reads, Prompt rendering, Completion, MRTR
binding, and result limits.

```console
cargo test -p stateknot-integrations mcp_server_ --locked
bash conformance/mcp-server/run-2026-07-28.sh
```

The official fixture intentionally mirrors the conformance inventory and uses
the production `McpServerHttpService` transport. It is acceptance evidence, not
an application template. The StateKnot-owned registry and policy layer is
covered by the separate HTTP tests above. Read the exact inventory and claim
rules in [MCP conformance status](mcp-conformance.md).

## Not claimed

- MCP Tasks, task lifecycle, task notifications, or Task/MRTR composition;
- deprecated stateful sessions or the legacy `initialize` flow;
- a bundled OAuth authorization server or identity provider;
- dynamic catalog mutation or list-changed notifications;
- dynamic MCP Skills manifests, MCP Apps, or other extensions;
- a stable Rust API, crates.io release, or SDK-tier certification;
- production qualification of the complete StateKnot framework.
