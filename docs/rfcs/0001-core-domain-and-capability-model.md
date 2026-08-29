<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0001: Core domain and capability model

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-08-28
- Tracking issue: Not yet created
- Supersedes: None
- Superseded by: None

## Summary

This RFC defines the stable domain boundary shared by the embedded SDK,
durable runtime, model and tool integrations, server API, MCP adapter, and A2A
adapter. It standardizes identifiers, content, messages, artifacts, schemas,
capabilities, identity, budgets, execution context, errors, callable model/tool
boundaries, and immutable agent definitions.

The core model is provider-, protocol-, database-, and async-runtime-neutral.
Provider SDK types, MCP/A2A wire types, SQL records, Axum extractors, and Tokio
primitives cannot appear in its public contracts.

## Motivation

StateKnot needs one durable vocabulary before graph, persistence, provider, and
protocol code can be implemented independently. Reusing one integration's
types would make its versioning and semantic compromises the framework's public
API. Passing arbitrary JSON maps through every boundary would make schema
validation, migration, redaction, capability checks, and deterministic hashing
unreliable.

The three qualification scenarios require the same values to retain meaning
across in-process calls, database records, event streams, protocol mappings,
crash recovery, and audit export. This RFC establishes those meanings without
pre-designing excluded v1 features.

## Goals and non-goals

### Goals

- make tenant, identity, version, budget, provenance, and schema boundaries
  explicit and difficult to omit accidentally;
- provide ergonomic typed model and tool authoring while allowing heterogeneous
  capabilities to be stored and invoked through validated erased adapters;
- distinguish trusted instructions, untrusted messages, external artifacts,
  model requests, model results, tool calls, and tool results;
- provide capability negotiation before side effects occur;
- define stable error categories and retry/ambiguity semantics;
- define canonical serialization rules for hashing, approval binding,
  idempotency, durable envelopes, and cross-version fixtures;
- prevent secrets and transport-specific authentication objects from becoming
  serializable domain data.

### Non-goals

- graph topology, superstep, reducer, scheduler, or interrupt state-machine
  semantics, which belong to RFC-0002;
- SQL schema, transactions, leases, outbox, checkpoint layout, and recovery,
  which belong to RFC-0003;
- MCP and A2A wire mappings or OAuth flows, which belong to RFC-0004;
- a built-in RAG, vector store, prompt-template language, workflow DSL, or
  arbitrary metadata-driven plugin system;
- a universal least-common-denominator provider API that hides supported
  provider extensions.

## Design principles

1. **Typed invariants, bounded extension points.** Required semantics use named
   Rust types. Namespaced JSON extensions exist only where integration-specific
   data is unavoidable and are size-limited.
2. **Domain and wire separation.** Adapters perform explicit, fallible mapping.
   A remote task is not an internal run, and a provider tool call is not a
   committed StateKnot invocation.
3. **Trust is metadata, not a prompt convention.** Content records its source,
   trust class, security label, and provenance separately from its text.
4. **No implicit unlimited execution.** A runnable context carries a resolved
   deadline and finite system-enforced budget even when the caller omits limits.
5. **Ambiguity is a result.** An external write whose outcome cannot be proven
   is not converted into a retryable failure or a fictitious success.
6. **Canonical forms are explicit.** Display formatting and ordinary Serde JSON
   are not used as approval, idempotency, or integrity hashes.

## User-facing design

The examples describe the intended API shape. They become normative only after
the M0 contract examples compile against the implementation.

### Typed tool authoring

```rust,no_run
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stateknot::prelude::*;

#[derive(Debug, Deserialize, JsonSchema)]
struct RestartInput {
    service: String,
    region: String,
    expected_revision: String,
}

#[derive(Debug, Serialize, JsonSchema)]
struct RestartOutput {
    deployment_id: String,
    accepted_revision: String,
}

struct RestartService {
    descriptor: ToolDescriptor,
    deployment_api: DeploymentClient,
}

impl Tool for RestartService {
    type Input = RestartInput;
    type Output = RestartOutput;

    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(
        &self,
        ctx: ToolContext,
        input: Self::Input,
    ) -> BoxFuture<'_, Result<ToolOutput<Self::Output>, ToolError>> {
        Box::pin(async move {
            let key = ctx.required_idempotency_key()
                .expect("registration requires a runtime-provided key");
            let output = self.deployment_api.restart_with_key(key, input).await?;
            Ok(ToolOutput::inline(output))
        })
    }
}
# struct DeploymentClient;
# impl DeploymentClient {
#     async fn restart_with_key(
#         &self,
#         _: ToolIdempotencyKey,
#         input: RestartInput,
#     ) -> Result<RestartOutput, ToolError> {
#         Ok(RestartOutput { deployment_id: String::from("deployment-42"), accepted_revision: input.expected_revision })
#     }
# }
# fn main() {}
```

Descriptor construction is fallible because capability names, versions, scopes,
descriptions, timeouts, schemas, and extension sizes are validated. The tool
binding owns its capability-scoped dependency client; raw credentials do not
enter `ToolContext`. The typed tool is wrapped by an erased adapter only after
both generated schemas pass local registry validation.

### Model invocation

```rust,no_run
use stateknot::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
struct IncidentSummary {
    summary: String,
}

async fn invoke(
    model: &dyn Model,
    ctx: ModelContext,
    instruction: Instruction,
    message: Message,
    output_schema: SchemaReference,
) -> Result<IncidentSummary, Box<dyn std::error::Error>> {
    let limits = ModelRequestLimits::new(
        TokenCount::new(8_192),
        TokenCount::new(1_024),
        ByteCount::new(1_048_576),
    )?;
    let request = ModelRequest::builder(limits)
        .instruction(instruction)
        .message(message)
        .text_output_format(Some(ModelTextOutputFormat::json_schema(output_schema)))
        .build()?;

    model
        .descriptor()
        .capabilities()
        .satisfies(request.requirements())?;
    let response = model.invoke(ctx, request).await?;
    Ok(response.decode_structured()?)
}
```

Capability mismatch fails before provider invocation. Provider-specific options
may be supplied through a registered, namespaced extension, but cannot weaken
the normalized `ModelRequirements` derived by `ModelRequest::build`, budgets,
policy, schema validation, or durable guarantees. The digest-pinned output
schema must also pass the selected adapter profile; feature negotiation alone
is not schema validation.

### Bounded run context

```rust,no_run
use stateknot::prelude::*;

fn inspect(ctx: &RunContext) {
    let tenant: &TenantId = ctx.tenant();
    let principal: &Principal = ctx.principal();
    let deadline: Timestamp = ctx.deadline();
    let remaining: BudgetRemaining = ctx.budget().remaining();
    let cancelled: bool = ctx.cancellation().is_cancelled();

    // Credentials are opaque, asynchronously resolved for one capability, and
    // intentionally cannot be serialized or formatted with Debug.
    let credentials: &CredentialResolver = ctx.credentials();
}
```

## Module and dependency boundary

The proposed `stateknot-core` modules are:

```text
artifact     budget       capability   content      error
extension    identity     ids          model        schema
time         tool         version
```

The core crate may depend publicly on `serde`, `serde_json`, `schemars`, and
`futures-core`, plus small implementation dependencies for UUID, time, hashing,
and error support. It MUST NOT depend on provider SDKs, MCP/A2A SDKs, Axum,
Tower, SQLx, Tokio, tracing subscribers, OpenTelemetry exporters, or object-store
clients.

The facade crate re-exports a deliberately small prelude. It does not glob
re-export integration SDKs.

## Identifiers

### Tenant and external subjects

`TenantId`, `SubjectId`, issuer identifiers, capability names, and external
references are validated opaque strings. Each type defines a maximum UTF-8 byte
length and allowed grammar. They are not interchangeable `String` aliases.

`TenantId` permits 1–128 ASCII characters from `[A-Za-z0-9._:-]`. Empty,
whitespace-containing, path-like, control, or normalization-ambiguous values are
rejected. An application maps its external tenant identifier into this form
before calling StateKnot.

`IssuerId` is a case-sensitive, absolute HTTPS URI of at most 512 ASCII bytes.
It requires a host, permits an optional valid `u16` port and path, and rejects
userinfo, query, and fragment components. It preserves the exact input and does
not lowercase, remove a default port or trailing slash, resolve dot segments,
or normalize percent encoding. [OIDC Core](https://openid.net/specs/openid-connect-core-1_0.html)
and [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414.html) require exact issuer
comparison, while [JWT StringOrURI values](https://www.rfc-editor.org/rfc/rfc7519.html#section-2)
forbid transformation before comparison; URI-equivalent strings are therefore
distinct security domains.

`SubjectId` is a non-empty, case-sensitive opaque value of at most 255 printable
ASCII bytes. The length and ASCII constraints follow OIDC Core; StateKnot
rejects control bytes to preserve supported database and text boundaries. It
is never trimmed, normalized, interpreted as a username, or emitted through
its `Debug` implementation.

`PrincipalIdentity { issuer, subject }` is the stable external identity key.
[OIDC Core section 5.7](https://openid.net/specs/openid-connect-core-1_0.html#ClaimStability)
guarantees stability and uniqueness only for that pair. A bare subject is never
global, and tenant-scoped storage and authorization still include the separate
`TenantId`.

### StateKnot-generated IDs

`RunId`, `ThreadId`, `EventId`, `FailureId`, `MessageId`, `ArtifactId`,
`InvocationId`, `InterruptId`, and `AttemptId` are distinct newtypes generated
from UUIDv7 values. Their canonical wire form is lowercase hyphenated UUID
text. Parsing accepts only the canonical form for security-bearing and durable
APIs; human-facing CLI input may offer a separate permissive parser.

An ID never conveys authorization. Storage keys and lookups include `TenantId`,
and authorization is evaluated before revealing whether an ID exists.

## Time, duration, money, and hashes

- `Timestamp` is a UTC instant serialized as RFC 3339 with exactly six
  fractional decimal digits and a trailing `Z`.
- persisted durations and deadlines use signed 64-bit integer milliseconds with
  validated non-negative domain wrappers where negative values are invalid;
- execution, token, and byte counters use distinct domain types and checked
  `u64` arithmetic;
- known cost uses a non-negative `u64` count of micro-units plus an uppercase
  ISO 4217 alphabetic currency code; floating-point currency is forbidden and
  arithmetic across different currencies is rejected;
- integrity values use `Digest { algorithm, bytes }`, with SHA-256 mandatory in
  v1 and canonical text `sha256:<lowercase hex>`;
- random jitter and wall-clock reads are runtime services whose observed values
  are recorded when they influence a durable decision.

`Timestamp` accepts exactly `YYYY-MM-DDTHH:MM:SS.ffffffZ`, covering UTC years
`0000..=9999`. It rejects alternate offsets, variable fractional precision,
leap-second text, and conversions that would silently discard nanoseconds.
`stateknot-core` exposes fallible `std::time::SystemTime` conversions; the
calendar implementation dependency remains private and is not part of the
public compatibility surface.

Full-width 64-bit non-negative values use canonical decimal strings on JSON
boundaries: `0` or a non-zero ASCII digit followed by ASCII digits, with no
sign, whitespace, exponent, decimal point, or leading zero. This applies to
`DurationMillis`, `TokenCount`, `ByteCount`, and `Money.micro_units`. The Rust
representation remains an integer. String encoding preserves exact values in
JavaScript and follows the same interoperability rationale as ProtoJSON's
`int64`/`uint64` mapping; RFC 8259 only guarantees exact agreement for JSON
integers through `2^53 - 1`.

`CurrencyCode` validates the stable ISO 4217 three-uppercase-ASCII-letter
structure. Whether a code is current, historical, a fund, or permitted by a
tenant/provider is mutable reference data and is checked at configuration and
ingestion boundaries against a versioned catalog. Durable readers retain
syntactically valid historical codes instead of consulting a live registry,
so an ISO maintenance update cannot make old events unreadable.

`Money` serializes as an object with exactly `currency` and `micro_units`.
Unknown cost is represented by absence at the enclosing usage layer, never by
zero money. `Money` does not define ordering or conversion between currencies;
those operations require an explicitly versioned exchange-rate service.

## Version model

`Version` is a validated semantic version with numeric major, minor, and patch.
Durable execution pins independently:

- graph version;
- state-schema version;
- event/payload schema version;
- model adapter and provider model identifier;
- prompt/instruction version;
- tool capability version;
- policy version; and
- protocol profile version.

An opaque provider model identifier such as a dated model name is not parsed as
semantic version. It is stored separately from adapter version.

## Content and messages

### Content parts

The core content enum is closed for v1:

```rust
pub enum ContentPart {
    Text(TextContent),
    Json(JsonContent),
    Artifact(ArtifactRef),
}
```

Its durable JSON representation is an adjacent-tagged closed object with the
exact fields `type` and `content`; the stable tags are `text`, `json`, and
`artifact`. Missing, additional, or unknown fields and tags are rejected.

Image, audio, and file inputs use `ArtifactRef` with a validated media type,
size, digest, and modality. Bytes and arbitrary remote URLs are not embedded in
durable messages. Embedded callers first register bytes or an external reference
through the configured artifact boundary, where size, URL, MIME, hash, tenancy,
and egress policy can be enforced.

`TextContent` records text plus language, source, trust classification, security
label, and redaction metadata. `JsonContent` contains a schema identifier when
known and a `BoundedJson` value. The default v1 boundary accepts at most 256 KiB
of raw or compact JSON, 32 array/object levels, 1,024 entries in any one
container, 16,384 total value nodes, 64 KiB in one decoded string, and 256 bytes
in one decoded object key. Configuration may narrow those values but cannot
exceed the v1 hard ceilings of 2 MiB, 64 levels, 8,192 container entries,
131,072 nodes, 1 MiB per string, and 1,024 bytes per key.

Untrusted JSON is validated by a streaming Serde visitor rather than first
being collected into `serde_json::Value`. The raw byte ceiling is checked before
parsing; depth, node, container, decoded-string, decoded-key, and compact-byte
ceilings are enforced while values are visited. A node or container violation
stops before an excess child is traversed. The parser accepts exactly one JSON
value and rejects duplicate object names after escape decoding, as required by
[I-JSON](https://www.rfc-editor.org/rfc/rfc7493.html#section-2.3). This avoids the
implementation-dependent last-key-wins behavior described by
[RFC 8259](https://www.rfc-editor.org/rfc/rfc8259.html#section-4).

`BoundedJson` is an immutable resource-safety substrate, not schema validation
and not JSON canonicalization. Generic nested `Deserialize` use enforces the
semantic and compact limits but cannot observe an enclosing transport's raw
whitespace; HTTP, protocol, and storage adapters therefore enforce their body
or record byte ceiling before invoking Serde. Conversion from an existing
`serde_json::Value` is restricted to trusted in-process data because duplicate
names have already been lost.

`LanguageTag` accepts well-formed RFC 5646 language tags, grandfathered tags,
and private-use tags up to 255 ASCII bytes. It rejects duplicate variant and
extension-singleton subtags and stores one lowercase representation because
RFC 5646 comparison is case-insensitive. Parsing is deliberately independent
of the mutable IANA Language Subtag Registry: StateKnot neither rejects a
durable record because a registry snapshot changed nor silently rewrites it to
a registry-version-dependent preferred value.

`SecurityLabel` is an opaque, case-sensitive policy-engine key of 1 to 128
ASCII bytes. It starts with an alphanumeric byte and subsequently permits
letters, digits, `.`, `_`, `:`, `/`, and `-`. Core defines no public/default
label, ordering, clearance relation, or declassification behavior. Each
`ContentMetadata` value explicitly records `source`, `trust`, `security_label`,
and `redaction`; all four fields are mandatory and unknown fields are rejected.
The stable v1 source values are `application`, `user`, `model`, `tool`,
`remote_agent`, and `artifact`; trust values are `application_controlled` and
`untrusted`; redaction values are `not_applied`, `partial`, and `full`.
`not_applied` does not mean non-sensitive. All metadata is an auditable claim,
not authority: deserializing `application_controlled` cannot construct an
`Instruction`, authorize execution, declassify content, or enable logging.

`TextContent` contains 1 to 262,144 UTF-8 bytes and preserves the exact bytes;
it performs no trimming or Unicode normalization. It rejects C0/C1 controls
except tab, line feed, and carriage return, and rejects every Unicode
noncharacter. Other valid Unicode, including bidi formatting characters,
remains data that output-context renderers must escape safely. `Debug` reports
only byte length, language, and security metadata, never the text. `JsonContent`
applies the same mandatory metadata to `BoundedJson`; its optional
`SchemaReference` is a digest-pinned declaration and does not itself perform
schema lookup or validation.

### Instructions and messages

Trusted instructions and conversation messages are separate types:

- `Instruction` is created only from application-controlled configuration and
  records a stable name/version identity, exact text or immutable artifact,
  content digest, and owner provenance;
- `Message` has `User`, `Assistant`, or `Tool` role plus a durable `MessageId`,
  bounded ordered content parts, run/event causation, and typed producer
  provenance;
- provider-specific system/developer roles map from ordered `Instruction`
  records at the adapter boundary;
- untrusted retrieved, MCP, A2A, file, and tool content cannot be converted into
  `Instruction` without an explicit application policy decision.

`InstructionName` is a case-sensitive 1–128 byte ASCII name that begins with an
alphanumeric byte and subsequently permits letters, digits, `.`, `_`, and `-`.
`InstructionIdentity` binds that name to a `Version`; `InstructionProvenance`
identifies the principal owning the instruction namespace. Text instructions
must have immediate source `application` and trust `application_controlled`.
Artifact instructions retain source `artifact` and must also be
application-controlled. The stored content digest covers exact UTF-8 text bytes
or the referenced immutable artifact bytes and is revalidated on deserialize.
Structured JSON is not an instruction variant: applications render it into
validated text or register it as an immutable artifact first.

These checks prevent accidental promotion inside trusted application code, but
a serialized owner or `application_controlled` claim is never proof of authority.
Untrusted API/protocol bodies cannot directly deserialize into an executable
instruction path; the configured owner registry and policy select the pinned
instruction record.

`MessageRole` is closed to `user`, `assistant`, and `tool`; system/developer
authority is deliberately absent. `MessageProducer` records exactly one of:

- an authenticated principal for a user message;
- a durable model attempt for an assistant message;
- an owner-qualified, version-pinned agent/workflow/application capability for
  an assistant message; or
- an owner-qualified, version-pinned capability plus `InvocationId` for a tool
  message.

Role and producer combinations outside that matrix are rejected. Non-artifact
content sources are also checked against the role: user accepts direct user,
remote-agent, or application content; assistant accepts model, remote-agent, or
application content; tool accepts only tool content. Registered artifacts can
be attached to every role because their own provenance remains authoritative.
Trust metadata remains an auditable claim and does not become safer merely
because a part is mapped to a user or assistant provider role.

`MessageParts` preserves order, requires 1–64 parts, and accepts at most 2 MiB
of aggregate materialized text plus compact JSON. Artifact bytes remain outside
that total and are enforced by the artifact resolver. Streaming deserialization
stops at the first part-count or aggregate violation. Runtime configuration and
provider capability profiles may narrow these hard v1 ceilings before
invocation.

[OpenAI Responses](https://platform.openai.com/docs/api-reference/responses),
[Anthropic Messages](https://platform.claude.com/docs/en/build-with-claude/working-with-messages),
[Gemini GenerateContent](https://ai.google.dev/api/generate-content), and
[Amazon Bedrock Converse](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html)
do not share one instruction-role model. A2A 1.0 roles are directional user/agent
values, while MCP sampling historically represented tool results as user-role
messages and is deprecated in MCP 2026-07-28. Adapters therefore map these
semantics explicitly, retain provider/protocol IDs in bounded adapter records,
and fail capability negotiation when an ordered instruction cannot be
represented without changing authority.

## Artifacts and provenance

`ArtifactRef` contains:

- `ArtifactIdentity { tenant_id, artifact_id }` so no artifact ID is resolved
  outside an explicit tenant;
- `ArtifactPresentation` with a required logical name and optional description;
- `ArtifactRepresentation` with media type, modality, byte length, digest, and
  optional digest-pinned structured-content schema;
- mandatory `ContentMetadata` whose immediate source is exactly `artifact`;
- an opaque, case-sensitive retention-policy class;
- `ArtifactProvenance` with creator principal, optional pinned capability
  name/version, and causing run/event IDs; and
- a sorted, unique set of at most 32 direct parent artifact IDs.

`MediaType` accepts a concrete media type, not a wildcard media range. Type,
subtype, and parameter names follow the RFC 6838 restricted-name grammar; type,
subtype, names, and `charset` values are lowercase, while other parameter value
case is retained. Parameter names are unique and sorted, quoting is
deterministic, the canonical value is at most 512 bytes, at most 16 parameters
are accepted, and each decoded parameter value is at most 128 ASCII bytes.
Validation is intentionally independent of the mutable IANA registry. Declared
media type and modality are interpretation hints, never authorization or a
substitute for policy-controlled byte inspection; adapters reject unsupported
combinations before provider invocation.

Artifact names are 1 to 255 UTF-8 bytes and preserve exact text, but reject
path separators, `.`/`..`, leading or trailing Unicode whitespace, controls,
and Unicode noncharacters. They are display names and must never be used as
filesystem paths. Descriptions are 1 to 4,096 UTF-8 bytes and use the same
control policy as `TextContent`. Both redact their contents from `Debug`.

`ArtifactRepresentation` requires a zero-length value to carry the known
SHA-256 digest of empty bytes. The resolver separately enforces the declared
length and digest while streaming; the metadata does not prove that a backing
object exists. A schema reference is accepted only for the `structured_data`
modality. The retention class grants no deletion, declassification, or legal-
hold authority by itself; a pinned runtime policy interprets it.

It does not expose storage credentials, bucket names, filesystem paths, or a
permanent public URL. A runtime artifact resolver authorizes access and returns a
bounded stream or short-lived handle appropriate for the adapter. Deserialization
revalidates all nested invariants and rejects unknown fields. Self-parent links
are rejected in core; cross-record existence, same-tenant lineage, and cycle
detection are atomic responsibilities of the artifact registry.

Provenance is append-only attribution. Sanitizing or transforming an artifact
creates a new artifact and links to its parents; it does not overwrite origin.

A2A 1.0 `Part` supports protocol-native text, data, raw base64 bytes, and URLs;
MCP 2026-07-28 content blocks support inline text/image/audio, embedded
resources, and resource links. Protocol adapters validate body/base64/redirect
limits and SSRF/egress policy, then ingest every binary or external resource
through the artifact boundary before constructing core `ContentPart`. A2A/MCP
metadata, filenames, resource URIs, and external artifact IDs remain in the
adapter's bounded protocol envelope or audit mapping. They do not become
permanent storage coordinates in core.

## JSON schemas

Tool inputs, tool outputs, structured model output, and structured artifacts use
JSON Schema Draft 2020-12. `JsonSchemaDocument` validates:

- a bounded encoded size and nesting depth;
- one canonical `$id` controlled by StateKnot or the owning capability;
- supported keywords and reference resolution policy;
- no network resolution during validation;
- stable schema digest and explicit version.

`SchemaId` is a normalized absolute HTTPS URI of at most 512 ASCII bytes. It
rejects relative references, user information, queries, fragments (including
an empty fragment), non-HTTPS schemes, and any spelling changed by RFC 3986
normalization. The URI is an identifier only: core and runtime code never
dereference it over the network. Schema bytes are resolved from an explicitly
populated local registry after digest verification.

Durable references use `SchemaReference { id, version, digest }` and reject
unknown JSON fields. The version is explicit even when the URI path also
contains a version, and the digest covers the canonical schema bytes. This
prevents mutable content at a reused URI from changing validation semantics.

Typed Rust tools normally generate schemas through `schemars`. A schema snapshot
is a compatibility artifact and changes when the corresponding capability
version changes. Input validation happens before policy and invocation; output
validation happens before a result is committed as successful.

## Capability model

`CapabilityName` is a case-sensitive string of 1–128 ASCII letters, digits,
`_`, `-`, or `.`. This freezes the tool-name recommendation in the
[MCP 2026-07-28 schema](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/schema/2026-07-28/schema.ts)
as a mandatory StateKnot v1 invariant. Names are unique within an owning
registry, not globally. Registry merges use owner/provenance identity and
reject collisions; they never silently rename a capability.

An A2A `AgentSkill.id` has a less restrictive protocol grammar. The A2A adapter
therefore maps it explicitly to a configured `CapabilityName`, retains the
external identifier as provenance, and rejects an unmapped or colliding value.
It does not normalize an arbitrary remote identifier into a local executable
name.

`CapabilityIdentity { owner, capability }` makes a durable reference
owner-qualified and version-pinned. `owner` is a `PrincipalIdentity`, while
`capability` is a `CapabilityReference { name, version }`. The serialized owner
is an auditable registry claim, not authentication, registration proof, or
authorization. A bare name/version pair is valid only when the surrounding
record already pins the registry namespace.

`CapabilityKind` is closed to `model`, `tool`, `agent`, `workflow`, and
`application`. `CapabilityMetadata` is the deliberately small discovery record
shared by specialized descriptors. Its exact fields are:

- owner-qualified `identity` and closed `kind`;
- optional `CapabilityTitle`, limited to 256 UTF-8 bytes, and mandatory
  `CapabilityDescription`, limited to 16 KiB;
- validated `CapabilityLifecycle`;
- a bounded, duplicate-rejecting `required_scopes` set;
- bounded namespaced `extensions`.

Titles are single-line. Descriptions may contain internal tab and CR/LF. Both
preserve exact UTF-8, reject boundary whitespace, bidi formatting controls and
Unicode noncharacters, and redact their text from `Debug`; titles additionally
reject every control and Unicode line separator. These checks prevent ambiguous
audit/display forms but do not make remote text trusted or replace
output-context escaping.

`CapabilityLifecycle` is a validated closed union:

- `active` has no additional fields;
- `deprecated` requires `announced_at` and a bounded migration `notice`, and may
  carry a `sunset_at` strictly later than the announcement plus an
  owner-qualified `replacement`;
- `retired` requires `retired_at` and a bounded notice, and may carry a
  replacement.

A metadata record rejects an exact self-reference as its replacement. A sunset
timestamp is publication data, not an implicit wall-clock scheduler: the
registry snapshots and enforces availability under policy. Retired records stay
decodable for audit and recovery but cannot be selected for new execution.

The common record intentionally excludes modalities, input/output schemas,
risk, provider features, execution limits, examples, tags, and transport
security schemes. Those fields have different semantics and requiredness in
specialized model, tool, and agent descriptors. This boundary follows the
actual interoperability surface: an
[MCP 2026-07-28 tool](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
has name/title/description, schemas, and explicitly untrusted annotations;
[A2A 1.0 `AgentSkill`](https://a2a-protocol.org/latest/specification/#445-agentskill)
adds tags, examples, modalities, and security requirements; and the
[OpenAI](https://developers.openai.com/api/docs/guides/function-calling) and
[Anthropic](https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools)
tool contracts expose different schema subsets and provider controls. Adapters
therefore map identity/title/description into common metadata and retain the
rest in the appropriate typed descriptor or bounded adapter envelope.

`CapabilityMetadata` is not itself executable. A trusted tenant registry must
authenticate the owner, pin the exact version, apply policy, validate any
registered extension semantics, and snapshot the specialized descriptor for an
execution attempt. Execution then uses the corresponding model, tool, agent,
workflow, or application boundary; unlike operations do not pretend to share
one invocation contract.

### Model capabilities

`ModelCapabilities` describes one exact model, adapter, API surface, and endpoint
binding. It is not a timeless assertion about a model family: the same weights
can expose different features through OpenAI-compatible, Anthropic Messages,
Bedrock Converse, regional, or hosted endpoints.

`ModelDescriptor` is the immutable specialized registry record. It contains
exactly common `CapabilityMetadata` whose kind is `model` and one validated
`ModelCapabilities` snapshot. Its owner-qualified, version-pinned StateKnot
identity is the stable registry key. Provider model IDs and aliases, API
surfaces, endpoints, regions, credential handles, and adapter configuration are
held in the trusted registry's versioned execution binding behind that key and
are snapshotted with the attempt. Changing that binding or its capabilities
requires a new StateKnot capability version; an existing descriptor is never
rewritten in place.

This avoids inventing false common semantics. The
[OpenAI model object](https://platform.openai.com/docs/api-reference/models/object)
only exposes a basic ID, creation time, and owner; the
[Anthropic Models API](https://platform.claude.com/docs/en/api/models/retrieve)
can resolve aliases to model IDs; [Gemini](https://ai.google.dev/gemini-api/docs/models)
distinguishes stable, preview, latest, and experimental names with different
drift guarantees; and [Bedrock](https://docs.aws.amazon.com/bedrock/latest/userguide/foundation-models-reference.html)
accepts base IDs, ARNs, provisioned resources, and inference profiles. These raw
identifiers may be recorded as bounded adapter provenance but are not a portable
core identity or authorization claim.

The snapshot records:

- non-empty, sorted input and output `ModelModalities` chosen from text, image,
  audio, video, and document;
- response streaming support;
- `ModelToolCapabilities` with a digest-pinned accepted schema profile, finite
  tool-definition and per-response tool-call ceilings, supported auto/none/
  required/specific choices, and strict-argument support; a per-response call
  ceiling greater than one is the portable definition of parallel tool calling;
- structured output at the ordered `unsupported | json | json_schema` levels,
  with a digest-pinned accepted profile exactly for `json_schema`;
- explicit readable reasoning-summary support, never access to raw hidden chain
  of thought;
- independently known total-context, input, and output token ceilings. Each may
  be explicitly unknown; unknown fails a positive capacity requirement and is
  never interpreted as unlimited.

Modalities are coarse negotiation facts. Exact MIME types, image dimensions,
document/page counts, audio/video duration, request bytes, and provider-specific
limits remain in a validated adapter profile. Tool and structured-output schema
profile URLs are offline identities: a trusted local registry resolves their
bytes and verifies versions and digests before a request can be selected.

Supported tool calling requires text input and output. JSON and JSON Schema
output and readable reasoning summaries require text output. Tool support
without a schema profile, active fields on an unsupported capability, zero
supported capacity, and schema profiles attached to weaker structured-output
levels are rejected during construction and deserialization.

`ModelRequirements` is the normalized request-derived contract. It contains
required modalities and feature levels, finite tool capacities/choices, and
positive known-token minima. `ModelCapabilities::satisfies` returns a sorted,
bounded, non-empty `ModelCapabilityMismatch` containing every unmet dimension,
including the known available capacity where relevant. Mismatch wire data also
rejects duplicate dimensions and claims where available capacity already meets
the requirement. Actual request schemas must additionally validate against the
pinned profiles; feature negotiation alone never rewrites or weakens a schema.
`ModelRequest` computes this value from its validated fields; the canonical wire
form includes the derived value for auditability, and deserialization recomputes
and rejects any mismatch rather than trusting the serialized claim.

This separation follows current provider surfaces. OpenAI publishes context,
output, streaming, function-calling, structured-output, and image-input support
per model, permits multiple calls on supported models, and documents a strict
[JSON Schema subset](https://developers.openai.com/api/docs/guides/structured-outputs).
Anthropic independently controls
[parallel tool use](https://platform.claude.com/docs/en/agents-and-tools/tool-use/parallel-tool-use),
[strict tool schemas and structured output](https://platform.claude.com/docs/en/build-with-claude/structured-outputs),
and counts both request and generated output in its
[context window](https://platform.claude.com/docs/en/build-with-claude/context-windows).
The Gemini Models API exposes separate input/output token limits and supported
actions, while Amazon Bedrock exposes modalities and streaming support per
deployed model binding through `GetFoundationModel`.

Readable reasoning summaries are opt-in, provider-authored summaries. Opaque,
signed, or encrypted reasoning-continuation blocks required by a provider are
preserved only in its bounded adapter state and passed back unchanged; they are
not logged, interpreted, exposed as summaries, or converted into core content.
Pricing, rate limits, service tiers, regional availability, and provider knobs
remain versioned policy/adapter data because they can change without a model
version change.

Capability data is evidence from an adapter and may become stale. The runtime
snapshots it with the attempt so recovery can detect drift. It does not override
provider errors, tenant policy, configured safety limits, or the resolved finite
run budget.

### Tool capabilities

`ToolDescriptor` contains:

- common `CapabilityMetadata` whose kind is exactly `tool`;
- digest-pinned `SchemaReference` values for JSON Schema 2020-12 input and
  output contracts;
- validated `ToolExecutionSemantics`: `ToolRisk::{ReadOnly,
  IdempotentWrite, NonIdempotentWrite}`, its compatible idempotency mechanism,
  and optional status-query/compensation support;
- broad network/filesystem access requirements, credential use, and whether
  invocation-supplied dynamic code is executed;
- cooperative cancellation support and a finite maximum progress-event count;
- finite per-invocation timeout, per-version concurrency, compact input,
  inline result, artifact-count, and aggregate artifact-byte ceilings.

The trusted local schema registry resolves schema bytes, checks their digest,
validates the document as JSON Schema 2020-12, requires an object-root input
schema, and checks the target provider profile before registration. Schema URLs
are identities, not runtime fetch instructions. MCP tool annotations remain
untrusted input and are never promoted to these reviewed semantics solely
because a remote server supplied them.

The legal risk/idempotency combinations are deliberately closed:

- `ReadOnly + NotApplicable`;
- `IdempotentWrite + Intrinsic | RequiredKey`;
- `NonIdempotentWrite + Unsupported`.

A required idempotency key must be durably reused across attempts. Status-query
and compensation declarations must correspond to separately registered and
authorized implementation entry points before an executable adapter is
accepted. Compensation is not rollback, and cooperative cancellation never
proves that an external effect did not occur.

Resource declarations are requirements, never grants. Exact destinations,
paths, operations, and opaque credential handles live in a tenant-controlled
executor profile that can only narrow the descriptor. A read-only tool cannot
declare network or filesystem write access. Dynamic code is an orthogonal
resource requirement rather than a side-effect class; v1 routes it to an
independently operated sandbox profile or denies it.

Approval rules, pricing, and mutable tenant policy are intentionally not
embedded in the immutable descriptor. A versioned policy evaluates the
descriptor together with principal, arguments, destination allowlists, budgets,
and run context. Descriptor limits are intersected with system, tenant, policy,
and run limits and can never widen them.

`ReadOnly` is a security and semantic assertion by the tool owner, not an
inference from an HTTP method. Misdeclaring it is a tool defect detectable by
review and integration tests.

## Typed and erased tool boundary

The public authoring trait is typed:

```rust
pub trait Tool: Send + Sync + 'static {
    type Input: serde::de::DeserializeOwned + schemars::JsonSchema + Send + 'static;
    type Output: serde::Serialize + schemars::JsonSchema + Send + 'static;

    fn descriptor(&self) -> &ToolDescriptor;

    fn call(
        &self,
        context: ToolContext,
        input: Self::Input,
    ) -> BoxFuture<'_, Result<ToolOutput<Self::Output>, ToolError>>;
}
```

`ToolAdapter::new` snapshots the descriptor and asks a trusted offline
`ToolSchemaRegistry` to bind both generated Rust type schemas to their exact
digest-pinned contracts. StateKnot then exposes an object-safe `ErasedTool` used
by heterogeneous registries and the runtime. The erased interface is not the
recommended application-authoring API. It:

1. binds object-root bounded JSON input to the exact invocation and descriptor,
   then validates it against the registered input schema;
2. deserializes into `Tool::Input`;
3. invokes the typed implementation;
4. serializes, bounds, and validates `Tool::Output`;
5. returns a `ToolResult` whose inline JSON, artifact count/aggregate bytes,
   tenant, run, owner, capability version, logical invocation, and physical
   attempt are independently revalidated before durable commit.

`InvocationId` identifies the logical external operation and derives the
provider-facing `ToolIdempotencyKey`; it remains stable when a scheduler creates
a new `AttemptId`. `ToolError` records phase separately from
`ToolExternalEffect::{NotApplicable, NotStarted, NotApplied, Applied, Unknown}`.
`Unknown` and `FailureCategory::AmbiguousExternalOutcome` are an exact pair and
therefore require `RetryAdvice::ReconcileFirst`. A timeout, cancellation, MCP
error, or closed transport cannot by itself produce `NotApplied` evidence.

When the descriptor allows progress and the runtime supplies a durable sink,
`ToolContext::progress` exposes a cloneable reporter bound to the exact
invocation, attempt, and tool version. It assigns contiguous zero-based
sequences, requires strictly increasing completed units, freezes a declared
total, and enforces the descriptor event ceiling. Async emissions are serialized:
concurrent emission is rejected, while a sink failure or dropped in-flight
future permanently poisons the reporter so later events cannot hide a gap.
Progress is observation only and never commits a successful tool result.

No tool receives a raw provider request, database transaction, bearer token, or
unrestricted service locator.

## Model boundary

The object-safe `Model` trait exposes descriptor/capabilities plus unary and
streaming invocation through runtime-neutral futures and streams:

```rust
pub trait Model: Send + Sync + 'static {
    fn descriptor(&self) -> &ModelDescriptor;

    fn capabilities(&self) -> &ModelCapabilities {
        self.descriptor().capabilities()
    }

    fn invoke(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'_, Result<ModelResponse, ModelError>>;

    fn stream(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxStream<'_, Result<ModelEvent, ModelError>>;
}
```

`BoxFuture` uses only `std::future::Future`; `BoxStream` uses the minimal
`futures-core::Stream` trait. Both are pinned, `Send`, and borrow the model, so
the core contract selects no executor, reactor, timer, HTTP client, or provider
SDK. An adapter disables SDK-level automatic retries: every actual provider
exchange needs a separately created and budgeted `AttemptId`. Unary invocation
accepts only complete-mode requests and streaming invocation only streaming-mode
requests. A stream error is terminal and no item may follow it.

`ModelRequest` is an immutable, provider-neutral invocation value. Its v1
contract is deliberately finite and fail-closed:

- up to 32 unique, ordered application-controlled instructions with at most
  8 MiB of resolved text content; instruction artifacts must have the text
  modality;
- up to 256 ordered durable messages with unique IDs and at most 64 MiB of
  resolved content. Inline JSON negotiates as text; text, image, audio, video,
  and document artifacts map to the corresponding model modality, while
  structured-data, archive, and binary artifacts require explicit application
  or adapter preprocessing;
- up to 128 active or deprecated `ToolDescriptor` values, canonicalized by
  registry-local name. Names collide across owners and versions because they
  are provider-visible. Selection is exactly none, auto, required, or one
  supplied name; an active tool set requires a positive per-response call
  ceiling no greater than 1024, and may require strict complete arguments;
- a non-empty output modality set. Text output has exactly one explicit
  `text`, `json`, or digest-pinned `json_schema` format; non-text-only output
  has no text format;
- complete or streaming delivery, an explicit readable-reasoning-summary flag,
  positive input/output token ceilings whose sum is representable, and a
  caller content ceiling no greater than 64 MiB; and
- a bounded registered extension map. Every adapter validates the extensions
  it owns and rejects unsupported values rather than silently dropping them.

Construction derives input modalities, tool/structured-output requirements,
streaming, reasoning, and the context/input/output token minima into
`ModelRequirements`. Deserialization repeats all collection and cross-field
validation and verifies the serialized derived requirements exactly. Capability
negotiation therefore cannot be bypassed by wire tampering.

This wire form is a durable internal contract, not an authorization protocol.
Serde validation cannot authenticate an instruction owner or tool registry
claim. Public API, MCP, and A2A adapters must ignore caller attempts to supply
trusted instructions or executable descriptors and instead resolve authorized,
version-pinned values from the tenant registry.

Portable core v1 intentionally fixes one candidate and no implicit truncation.
Provider conversation IDs, stored/background execution, service tiers,
temperature, top-p/top-k, stop sequences, and other provider controls do not
pretend to have common semantics; when supported they use registered,
validated adapter extensions. Deadline and cumulative token/cost/tool budgets
belong to `ModelContext` and the resolved run budget, not this per-invocation
value. A request never contains a provider SDK object or bearer credential.

`ModelResponse` is the immutable, ordered, provider-neutral result of exactly one
attempt. `ModelResponseProvenance` binds it to an `AttemptId` and the exact
owner-qualified `ModelDescriptor` identity. Optional provider model, request,
and response IDs preserve diagnostic correlation as opaque 1--512 byte
visible-ASCII values; they are redacted from `Debug` and never become registry,
authorization, replay, or idempotency keys.

The response preserves provider order across three `ModelOutputItem` variants:

- user-visible `ContentPart` values. Inline text/JSON must be
  `model + untrusted`; artifact references retain `artifact + untrusted`;
- explicitly requested, provider-authored readable reasoning summaries as
  `TextContent`, never hidden chain of thought; and
- complete `ModelToolCallProposal` values with an exact requested
  `CapabilityIdentity`, optional opaque provider call ID, bounded object-root
  JSON arguments, and bounded extensions required for registered provider
  continuation/signature data.

A proposal intentionally has no `InvocationId`. `AttemptId + ordered output
index` is its durable pre-authorization identity. The runtime assigns an
`InvocationId` only after resolving the exact tool from the authenticated tenant
registry, validating arguments against its digest-pinned input schema, and
passing policy, budget, approval, and invocation-ledger checks. Present provider
call IDs must be unique within the response but remain correlation data.

One response contains at most 256 content/summary items and 1024 complete tool
proposals, therefore at most 1280 ordered items. Aggregate retained inline text,
JSON, reasoning-summary, tool-argument, and per-proposal extension payload is
limited to 64 MiB. An artifact reference contributes no inline bytes; its declared external byte
length remains subject to artifact, run-budget, policy, storage, and downstream
resolver limits rather than forcing large media into the response allocation.

`ModelFinishReason` is the closed portable set `completed | tool_calls |
output_limit | context_limit | refused | content_filtered | paused`.
`tool_calls` requires at least one complete proposal, and every other reason
forbids executable proposals. Truncated or malformed tool fragments never become
proposals. Unknown provider terminal states, provider failures/cancellation, and
malformed model/tool output map to `ModelError`; adapters cannot call them
completed. Empty ordinary text completion is valid because a provider may bill
output tokens without a visible block, but structured completion still requires
exactly one typed `JsonContent`.

`ModelUsage` records required inclusive input and output token counts for the
attempt, plus optional cached-input and reasoning subsets. Absence of a subset
means unavailable, never zero. Subsets cannot exceed their inclusive totals and
input plus output uses checked arithmetic. OpenAI's input/output totals already
contain their cached/reasoning subsets; Anthropic input is normalized as base
input plus cache-creation plus cache-read tokens; Gemini input uses the effective
prompt total and output uses candidates plus thoughts; Bedrock uses its inclusive
input/output totals. Provider-specific cache write/read, modality, tool-prompt,
and service-tier breakdowns remain registered response extensions. An adapter
must reconcile any provider total instead of inventing or double-counting a
category, and a terminal result without accountable input/output usage is an
error rather than fabricated zero usage.

`ModelResponse::new` binds every value to the immutable descriptor and request
snapshot. It rejects a different model identity, usage above the request's input
or output ceiling, unrequested output modalities or reasoning summaries,
unknown/version-substituted tools, tool counts above the request ceiling, and
violations of required/specific selection. On nominal completion, plain text
cannot masquerade as typed JSON; `json` requires exactly one schema-free
`JsonContent`; `json_schema` requires exactly one `JsonContent` carrying the
same digest-pinned `SchemaReference`. The adapter must have already validated
the value against trusted local schema bytes. Non-complete terminal states may
carry partial text but are never decoded as structured success.

Deserialization repeats all intrinsic resource, metadata, usage, and
finish-reason validation. It cannot authenticate serialized descriptor/tool
claims or recover the request snapshot, so durable and remote values must call
`validate_for(descriptor, request)` before consumption. The adapter constructor
performs both layers automatically. This separation keeps historical values
readable without treating a wire claim as execution authority.

Streaming emits provider-neutral semantic `ModelEvent` values, not provider SSE
frames. Every envelope repeats its `AttemptId` and carries a zero-based
`ExecutionCount` sequence. Sequence zero must be `started`; every later sequence
is contiguous after the adapter has removed pings, empty deltas, transport
framing, and other non-semantic provider events. One attempt accepts at most
1,048,576 semantic events. This sequence orders model semantics only: it is not
the durable journal `EventId`, an SSE reconnect cursor, or an idempotency key.

The closed event body is `started | output_started | output_delta |
output_completed | usage_updated | completed`. `started` fixes the same
`ModelResponseProvenance` used by unary responses. Output starts register
zero-based positions contiguously in provider order; once registered, different
positions may receive deltas and close in interleaved order. An output header
fixes exactly one of text, JSON, external artifact reference, readable reasoning
summary, or tool call. Text/JSON/summary headers carry their final security and
language/schema metadata. Tool headers carry the exact requested capability
identity, optional provider call ID, and bounded extensions. Artifact headers
carry only a complete immutable reference and never accept inline binary/base64
deltas.

`ModelStreamChunk` retains exact non-empty UTF-8, rejects disallowed controls and
Unicode noncharacters, is redacted in `Debug`, and is limited to 64 KiB per
event. Text, JSON, reasoning-summary, and tool-argument delta kinds cannot be
cross-applied. JSON and tool fragments are allowed to be syntactically
incomplete while active; exact concatenated bytes are parsed once at
`output_completed`. The resulting JSON remains under the normal 256 KiB JSON
limits, tool arguments must be an object, and a proposal still receives no
`InvocationId`. Invalid or truncated tool arguments never become executable
proposals. Each active or completed item stays under its normal 256 KiB bound,
and active buffers plus completed response inline payload share the response's
64 MiB aggregate ceiling. A hard total item count prevents sparse-index or
allocation attacks.

Every `usage_updated` value is a complete cumulative per-attempt snapshot, not a
provider delta. Inclusive input/output counts and any already-known cached-input
or reasoning subset can only increase; a known optional subset cannot disappear.
Every snapshot is checked against the immutable request limits. A terminal
`completed` event repeats authoritative final usage, supplies the portable
finish reason and bounded response extensions, and must not leave an output
open. Provider pings have no core event. Provider failures, cancellation,
malformed output, unknown terminal states, and in-stream error frames remain
`Err(ModelError)` on the model stream rather than successful semantic events.

`ModelEventAccumulator` is the normative state machine. It binds an expected
attempt, descriptor, and streaming request; rejects the descriptor before any
event unless it satisfies every derived request requirement; checks attempts,
exact sequences, output lifecycle/type, resource accounting, provider call-ID
uniqueness, and usage monotonicity; then calls `ModelResponse::new` at the sole terminal event.
The accumulator owns that response and requires `finish()` after transport EOF,
so a disconnected stream cannot be confused with success. Any validation error
before terminal permanently poisons the accumulator and cannot be ignored to
resume at a later sequence. A canonical fixture proves that its terminal value
is byte-for-byte the same wire `ModelResponse` as the unary contract, rather than
a second streaming-only response type. Partial deltas are observable but never
committed as a complete response.

## Agent definition

An `AgentDescriptor` is an immutable, executable definition snapshot. It binds
one `kind=agent` capability version to exact input/output schemas, one resolved
`ModelDescriptor`, an ordered non-empty set of application-controlled
instructions, a canonically ordered set of resolved `ToolDescriptor` values, a
resolved structured-output strategy, finite loop limits, and an optional
agent-level budget layer. A reusable descriptor cannot embed the budget's
absolute `deadline`; the runtime derives that instant from run admission and
the applicable relative policy. A run snapshots this value; registry aliases, model
profiles, tool schemas, prompts, and lifecycle changes cannot silently alter an
already-started run.

StateKnot supports two durable structured-output strategies:

- `model_native` requires the pinned model binding to advertise JSON Schema
  output and requires the exact output schema to pass that binding's local,
  digest-pinned schema profile;
- `tool_call` reserves a framework-owned final-output tool definition. A final
  output call is accepted only when it is the sole completed call in that model
  response, and its arguments pass local schema validation. It is an output
  marker, never a user tool invocation, and therefore cannot create a tool
  ledger entry or external effect.

There is deliberately no serialized `auto` strategy. An ergonomic builder may
choose a strategy once while resolving a definition, but the resulting
descriptor and run snapshot record the exact choice. Model upgrades therefore
cannot change a recovered run from tool-based output to provider-native output
or the reverse.

`AgentExecutionConfig` has positive finite model-turn and per-response tool-call
ceilings, a finite output-repair allowance strictly below the turn ceiling, and
an explicit tool concurrency mode. `parallel_read_only` can parallelize only
tools whose trusted descriptor says `read_only`; writes remain serialized in
model proposal order. Regardless of completion order, tool results re-enter the
model transcript in proposal order. The effective run budget is still the
intersection of system, tenant, agent, and request layers and may stop the loop
before these descriptor ceilings.

Descriptor construction fails before execution when the pinned model cannot
accept every exposed tool definition, cannot emit the configured number of
calls, cannot implement the resolved output strategy, when tool names collide,
when a framework-reserved output name is shadowed, or when an instruction/tool
collection exceeds its hard count or byte limits. Schema references are
identities, not validation evidence: the trusted local registry must validate
input, output, tool, and provider-profile schemas before a descriptor becomes
selectable.

The ordinary agent loop is compiled by `stateknot-runtime` onto the same durable
graph/journal semantics as a hand-authored graph. Core does not expose an async
`Agent::run` implementation that could hide model attempts, tool effects,
approvals, or checkpoints inside one opaque future. Handoff and agent-as-tool
remain distinct orchestration operations: a handoff transfers task ownership,
whereas agent-as-tool returns control to the calling agent. Their run and
delegation records are specified by RFC-0002 through RFC-0004 rather than being
encoded as ordinary local tool success.

## Identity and delegation

`Scope` uses the case-sensitive RFC 6749 `scope-token` grammar: 1–256 visible
ASCII bytes excluding space, double quote, and backslash. The 256-byte ceiling
is a StateKnot resource bound. `ScopeSet` contains at most 128 unique scopes,
rejects duplicate input, and serializes as an array in exact ASCII byte order.
The array representation is the StateKnot domain wire form; OAuth adapters map
to and from the protocol's space-delimited parameter. See
[RFC 6749 section 3.3](https://www.rfc-editor.org/rfc/rfc6749.html#section-3.3).

`Principal` contains:

- `TenantId`;
- exact `PrincipalIdentity` issuer/subject pair;
- principal kind: user, workload, agent, or system;
- validated scope set;
- optional authenticated client/workload identity;
- ordered `DelegationChain`;
- authentication time and credential expiry metadata.

Each `DelegationHop` records delegator, delegate, granted scopes, audience,
reason, time bounds, and evidence reference. Effective scopes are the
intersection of tenant policy, authenticated scopes, every delegation hop, and
capability requirements. Delegation can narrow but never widen authority.
The `ScopeSet::intersection` operation implements this narrowing primitive; its
result is deterministically ordered and a subset of both operands.

Deserializing an `IssuerId`, `SubjectId`, or `PrincipalIdentity` only validates
its data shape; it never authenticates a caller. An identity adapter constructs
the principal only after validating the token signature and algorithm, exact
configured/discovered issuer, intended audience and authorized party, expiry
and not-before times, nonce/replay requirements, and tenant/provider policy.
Core identity types never perform discovery or network access.

The full principal is available to policy and audit. Model prompts receive only
explicitly selected, non-secret identity attributes.

## Budgets

`BudgetLimits`, `ResolvedBudget`, and `BudgetUsage` cover:

- wall-clock deadline;
- graph depth and steps;
- model attempts and turns;
- input, cached-input, reasoning, and output tokens where observable;
- tool calls, write calls, remote-agent delegations, and retries;
- concurrent branches and fan-out;
- input, output, event, checkpoint, and artifact bytes;
- known monetary cost per currency.

`BudgetLimits` is one partial layer: omitted fields mean that layer has no
opinion, never that execution is unlimited. The runtime supplies system,
tenant, policy, graph/capability, and caller layers as applicable.
`ResolvedBudget::resolve` accepts at most 16 layers, requires at least one, and
rejects the result unless every dimension has a finite value. It takes the
earliest deadline and the smallest applicable scalar limit, independent of
layer order. A caller can only narrow a system or tenant ceiling.

Known monetary ceilings are a deterministically ordered set of at most 16
`Money` values with unique currency codes. Omitting the entire cost field means
that layer has no opinion; when two layers provide it, their currency
allowlists are intersected and same-currency amounts take the minimum. An empty
resolved set permits no priced charge. This prevents a caller from introducing
a currency absent from system or tenant policy. A charge in an unlisted
currency is rejected rather than treated as unlimited, and core performs no
exchange-rate conversion. Any conversion or tenant base-currency reporting
requires a versioned external rate source.

`ExecutionCount`, `TokenCount`, and `ByteCount` use canonical decimal-string
wire values and checked `u64` arithmetic. `BudgetUsage` treats graph depth,
concurrent branches, and fan-out as high-water marks; other execution counts,
tokens, bytes, and known costs are monotonic totals. Write calls are a subset
of tool calls. Normalized input tokens include cached-input tokens, and
normalized output tokens include reasoning tokens. Adapters reconstruct those
inclusive totals where a provider reports components differently and retain
provider-specific breakdowns outside the core contract.

`BudgetUsage::checked_accumulate` takes maxima for high-water fields and checked
addition for totals. It is pure arithmetic, not a concurrency primitive.
`ResolvedBudget::remaining` requires an explicit observed clock value, treats
equality with the deadline as expired, and returns a typed error for the first
exceeded dimension. Known cost in an unbudgeted currency and any recorded
unpriced cost event both fail closed; absence of known price is never zero.

Run-cumulative input/output token budgets are separate from per-request context
and generation ceilings on `ModelRequest` and `ModelCapabilities`. This avoids
claiming preflight enforcement when a provider only reports exact token use
after billing. [Pydantic AI](https://ai.pydantic.dev/agent/#usage-limits)
similarly distinguishes cumulative and per-request limits, while
[LangGraph](https://docs.langchain.com/oss/python/langgraph/graph-api#recursion-limit),
[OpenAI Agents](https://openai.github.io/openai-agents-python/running_agents/),
[Google ADK](https://adk-labs.github.io/adk-docs/runtime/runconfig/),
[Microsoft Agent Framework](https://learn.microsoft.com/en-us/agent-framework/agents/looping),
and [AutoGen](https://microsoft.github.io/autogen/dev/user-guide/agentchat-user-guide/tutorial/termination.html)
expose narrower step/turn/iteration termination controls. Current provider
usage shapes also differ: [OpenAI Responses](https://developers.openai.com/api/reference/cli/resources/responses/methods/create)
includes cached input and reasoning breakdowns,
[Anthropic](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)
reports cache-read/cache-creation components, and
[Gemini](https://ai.google.dev/api/generate-content#usage_metadata) reports
cached content and thoughts separately. StateKnot adapters normalize those
shapes into the inclusive contract above.

Reservations prevent parallel branches from each consuming the full remaining
budget. Reservation, commit, release, recovery, and fencing are durable runtime
behavior specified by RFC-0003; the core arithmetic does not pretend an
in-memory counter can provide that guarantee.

## Execution contexts

`RunContext`, `ModelContext`, and `ToolContext` are capability-limited views,
not mutable property bags. `RunContext` carries:

- tenant, principal, delegation, scopes, and policy version;
- run, thread, trace, and correlation identifiers;
- deadline and cancellation signal;
- effective budget and usage view;
- credential resolver handle;
- clock and randomness services whose durable decisions can be recorded;
- invocation identity and access to safe progress/event emission.

`ModelContext` and `ToolContext` expose only the subset needed at that boundary.
They do not implement `Serialize`. Their `Debug` output is redacted and excludes
credential handles, tokens, content, and tool arguments.

The callable model boundary currently exposes tenant, run, thread, and attempt
identity plus a `BudgetRemaining` snapshot, cancellation signal, and two paired
clock observations. Construction converts the durable UTC budget deadline into
a process-local monotonic `Instant`; equality is expired, so wall-clock changes
cannot extend an in-flight call. Cancellation is checked before deadline when
both are observed, but neither condition proves that the provider did not
process or bill an already dispatched request. `CancellationObserver` is an
object-safe runtime adapter whose state is permanent and whose wait future must
be race-safe. A registered model binding owns its capability-limited credential
resolution; raw credentials never enter `ModelContext`.

The callable tool boundary additionally binds the immutable tool identity,
logical `InvocationId`, physical `AttemptId`, descriptor idempotency mechanism,
and already narrowed positive timeout. Its deadline is the minimum of that
timeout and the remaining run deadline. Required idempotency keys derive only
from `InvocationId`, so recovery reuses the same key across attempts. Contexts
remain non-serializable and validate back against the descriptor before erased
dispatch. An optional runtime progress reporter is similarly identity-bound,
finite, ordered, and fail-closed on any possible sequence gap.

`CredentialResolver` returns a non-serializable, non-cloneable or explicitly
zeroizing short-lived credential scoped to one named capability and audience.
Credential resolution is audited without recording secret material.

## Error model

Every public operation returns a typed component error containing a common
`Failure`:

```rust
pub enum FailureCategory {
    InvalidInput,
    Unauthenticated,
    PermissionDenied,
    PolicyDenied,
    NotFound,
    Conflict,
    Unsupported,
    RateLimited,
    DeadlineExceeded,
    Cancelled,
    DependencyUnavailable,
    DataCorruption,
    AmbiguousExternalOutcome,
    Internal,
}

pub enum RetryAdvice {
    Never,
    SafeAfter { delay: DurationMillis },
    ReconcileFirst,
}
```

`ModelError` adds only model-boundary evidence: the exact attempt/model identity,
optional opaque provider model/request/response IDs, an optional last complete
cumulative `ModelUsage` snapshot, and the closed phase `preparation | dispatch |
response | stream`. Phase records where observation failed; it does not imply
retry safety, provider billing certainty, or an external outcome. `response`
is valid only for complete delivery and `stream` only for streaming delivery.
`ModelError::validate_for` rebinds decoded errors to the exact context,
descriptor, request mode, and token ceilings. Missing usage remains unknown,
never zero. Mid-stream provider error frames, transport failure, cancellation,
deadline, malformed output, and EOF before the semantic terminal all end the
stream with this error and discard partial output as a completed response.

`ToolError` adds logical invocation/physical attempt/tool identity, a closed
preparation/execution/result phase, and explicit external-effect evidence.
Read-only failures use `NotApplicable`; writes cannot. A nominal typed `Ok`
asserts the operation completed, so a later output serialization/schema/result
failure is recorded as `Applied` for a write. Invalid or contradictory error
evidence from a write implementation is conservatively replaced with
`Unknown + AmbiguousExternalOutcome + ReconcileFirst`, never passed through as
retry-safe evidence.

Each occurrence has its own `FailureId`. `Failure` also contains a stable
machine code, origin, explicitly public-safe message, retry advice, optional
schema-bound public details, and optional causal `EventId`. Codes and origins
are 1–128 byte lowercase ASCII identifiers made from dot-separated segments;
every segment begins with `a-z` and then uses `a-z`, `0-9`, `_`, or `-`.
Messages are single-line UTF-8 values no larger than 1,024 bytes. Their
constructor rejects surrounding whitespace, controls, Unicode line separators,
bidirectional formatting controls, and noncharacters, but shape validation
cannot prove confidentiality: the caller remains responsible for omitting
secrets, prompts, private resource existence, provider payloads, stack traces,
and implementation details.

Structured public details always pair a `SchemaReference` with `BoundedJson`.
They are revalidated at 16 KiB compact JSON, depth 8, 64 entries per container,
512 total value nodes, 4 KiB per decoded string, and 128 bytes per decoded key.
The enclosing adapter also caps raw request/response bytes before Serde parsing
and validates the value against the locally registered, digest-pinned schema.
Details are not a generic extension map.

A private `Arc<dyn Error + Send + Sync>` source chain may be attached for
trusted in-process diagnostics. It is absent from serialization, JSON Schema,
and `Debug`; deserialization explicitly rejects a `private_source` member.
`std::error::Error::source` deliberately returns `None` so generic error-chain
formatters cannot leak it; trusted diagnostics use the explicit
`private_source()` accessor and a protected sink. The public `Display`
implementation emits only the approved message.

Retryability is never derived from category alone. The adapter or runtime
supplies explicit advice, and the scheduler intersects `SafeAfter` with the
operation's idempotency contract, resolved deadline and budget, attempt limit,
circuit breaker, and policy. A zero delay permits an immediate new attempt but
does not bypass those controls. `Never` describes automatic recovery for this
operation; it does not prohibit a separately authorized user action.

`AmbiguousExternalOutcome` and `ReconcileFirst` are an exact pair enforced by
construction and deserialization. This category cannot be converted to a
normal retry without a tool-specific status query, idempotency guarantee,
compensation decision, or human resolution. Other categories cannot use
`ReconcileFirst`; if reconciliation is required, the outcome is by definition
ambiguous.

Mappings are adapter-owned and fallible:

- HTTP APIs map authorized failures to
  [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) Problem Details. A stable
  problem-type URI represents `code`, an occurrence URI represents
  `FailureId`, and `detail` can use only the approved public message. The HTTP
  status and optional `Retry-After` are adapter decisions; the latter is emitted
  only for a compatible `SafeAfter`, never for `ReconcileFirst`.
- gRPC adapters map categories to the nearest status while preserving explicit
  recovery advice separately. Status-code names do not become retry policy;
  [gRPC itself requires configured retryable codes and retry limits](https://grpc.io/docs/guides/retry/).
- [A2A 1.0](https://a2a-protocol.org/latest/specification/) adapters map the
  internal failure to that specification's code/message/details model and then
  to JSON-RPC, `google.rpc.Status`, or HTTP+JSON as selected by the Agent Card.
  An adapter must preserve the A2A error type and binding-specific status
  without treating an internal category as an A2A wire code.
- [MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/basic/index)
  adapters preserve the distinction between JSON-RPC protocol errors and
  [tool execution errors](https://modelcontextprotocol.io/specification/2026-07-28/server/tools#error-handling).
  A model-actionable tool failure is not silently promoted to a protocol error,
  and a malformed protocol request is not presented as model tool output.

Every mapping applies authentication and existence-hiding policy before
selecting public status, code, message, and details. Protocol responses never
expose internal SQL/provider errors, credentials, prompts, stack traces, or the
private source chain. The original category, code, origin, advice, and failure
ID remain available only in appropriately redacted, tenant-scoped audit
evidence.

## Extensions

`Extensions` is a protocol-neutral, sorted map from `ExtensionKey` to an
explicitly tagged `ExtensionValue`. It is not a capability registry and
deserializing it never installs code, grants authority, or activates behavior.

An `ExtensionKey` is at most 512 encoded bytes and has exactly one of these
forms:

- a normalized absolute HTTPS identifier with a non-empty authority and no
  userinfo, query, or fragment;
- a normalized `urn:` identifier with a 2–32 byte lowercase namespace
  identifier and a non-empty namespace-specific value; or
- a lowercase reverse-DNS name containing at least three DNS labels, where
  every label is at most 63 bytes, begins with a letter, ends with a letter or
  digit, and otherwise contains only letters, digits, and hyphens.

URI keys are identities only and are never dereferenced. The reverse-DNS and
URI owners are responsible for stable names and for using a new versioned key
for breaking semantics. Core-owned keys use a `StateKnot`-controlled namespace.

`ExtensionValue` has two closed wire variants:

- `opaque` contains bounded JSON with no registered semantic contract;
- `schema_bound` carries an immutable `SchemaReference` plus bounded JSON.

`schema_bound` declares the intended schema identity; construction or
deserialization alone is not proof that validation succeeded. Before semantic
use, a boundary must resolve the schema from a trusted local registry, verify
its pinned digest, validate the value under a bounded execution budget, and
produce a separate typed/validated result. Unknown or opaque values never
participate directly in authorization, policy, capability selection,
idempotency, hashing, scheduling, or deterministic reduction.

The immutable v1 hard ceiling is 64 entries, 512 bytes per key, 256 KiB for the
exact compact JSON representation of the complete map, and
`JsonLimits::DEFAULT` for each value. The complete-map count includes key JSON
syntax and the tagged value/schema envelope. Empty `{}` is valid and accounts
for two bytes. Callers may construct a validated profile that narrows every
dimension but cannot widen the hard ceiling. Keys are serialized in canonical
byte order; duplicate keys, including duplicates encountered after JSON escape
processing inside values, are rejected before their duplicate value is
traversed. Generic Serde cannot account for whitespace outside the map, so every
transport and durable record reader must cap raw bytes before deserialization.

Extensions cannot:

- add scopes or bypass policy;
- alter tenant, identity, deadlines, budgets, risk, or idempotency semantics;
- contain raw credentials;
- participate in deterministic decisions unless their schema, canonical form,
  and version are registered and pinned;
- be silently forwarded across trust boundaries.

Unknown extensions are either preserved as opaque bounded data or rejected
according to the receiving boundary's negotiated profile.

Protocol adapters preserve their own wire rules instead of coercing every
metadata property into this map:

- [A2A 1.0](https://a2a-protocol.org/latest/specification/) declares and
  negotiates extensions by URI, including the `A2A-Extensions` service
  parameter, and uses extensions to type otherwise flexible metadata. A
  negotiated normalized HTTPS/URN identifier can map directly to an
  `ExtensionKey`; any other A2A URI stays in the bounded adapter envelope or is
  rejected. Breaking A2A extension semantics require a new URI, and an adapter
  never falls back to another version implicitly. The
  [A2A extension governance rules](https://a2a-protocol.org/latest/topics/extension-and-binding-governance/)
  also define canonical HTTPS URI namespaces as identifiers that are not
  expected to be fetched.
- [MCP 2026-07-28 `_meta`](https://modelcontextprotocol.io/specification/2026-07-28/basic/index#_meta)
  uses its own optional `prefix/name` grammar and reserves MCP and tracing keys.
  Raw `_meta` therefore remains adapter-owned. Only an explicit, collision-free
  registry mapping from one negotiated MCP key to one core `ExtensionKey` may
  promote its bounded value; adapters must not mechanically replace `/` with
  `.` or interpret self-reported metadata as a security decision.

## Canonical serialization

Durable and signed values use an envelope:

```json
{
  "schema": "https://stateknot.github.io/schema/run-event/1.0.0",
  "kind": "tool-call-requested",
  "data": {}
}
```

The `stateknot.github.io` authority is controlled by the GitHub organization and
keeps schema identity independent of a registrar or future marketing-domain
change. A schema identifier is stable identity; runtime validation MUST NOT
require network dereferencing.

Canonical bytes use RFC 8785 JSON Canonicalization Scheme after StateKnot schema
validation. JSON numbers must be finite and schema-bounded; money, 64-bit values
that cross JavaScript boundaries, timestamps, UUIDs, and digests use their
defined string or integer forms. Hashes include the schema identifier and kind.

The sorted object representation inside `BoundedJson` makes ordinary output
deterministic under StateKnot's dependency configuration, but MUST NOT be used
as RFC 8785 output or as approval, signature, digest, or idempotency bytes. The
canonicalization layer remains an explicit, separately tested operation after
schema validation.

Ordinary API JSON may be pretty-printed or reordered, but approval action
hashes, idempotency input hashes, schema digests, event checksums, and fixture
goldens always use canonical bytes.

Security-bearing commands reject unknown fields. Durable event readers may
ignore additive fields only within a supported schema-major version and must
retain the original canonical payload/checksum for audit. Unsupported major
versions fail explicitly and never fall back to best-effort decoding.

## Persistence and migration

This RFC defines serializable values but not table layout. RFC-0003 must ensure:

- every persisted domain payload carries its schema identifier and checksum;
- durable data uses explicit conversion records rather than serializing arbitrary
  Rust implementation structs directly;
- migrations transform from known schema versions and preserve provenance;
- unknown or corrupt payloads quarantine the affected run instead of causing
  unsafe execution or process-wide failure;
- N-1 and N-2 fixtures remain readable or have a documented offline migration.

## Security and privacy

- all constructors enforce length, count, depth, and canonical-form limits before
  allocation or downstream calls where practical;
- sensitive content is labeled and redacted by default; `Debug`, error, tracing,
  and metrics implementations use safe summaries;
- secret-bearing types do not implement `Serialize`, `Clone`, `Display`, or
  revealing `Debug` and use zeroization where the backing SDK permits;
- tenant and principal are required arguments for resource-bearing operations;
- untrusted content cannot become an instruction, tool definition, capability,
  extension policy, or credential reference through deserialization alone;
- URLs are represented as untrusted references and fetched only through the
  RFC-0004 egress boundary;
- capability schemas and descriptors have provenance and version pins to resist
  tool-definition and protocol supply-chain substitution.

## Observability and operations

Domain objects expose safe attribute projections rather than relying on generic
serialization. Required low-cardinality telemetry includes component, stable
operation, result category, retry advice, capability name/version, protocol
profile, and budget dimension. Tenant, run, model, tool, and remote identifiers
follow configurable cardinality and privacy policies.

Content, raw JSON, artifact names, external references, subjects, and error
source chains are not metric labels. Trace/event content capture is disabled by
default and, when enabled, applies field-level redaction and retention policy.

## Compatibility

- the initial MSRV is Rust 1.85.0;
- public Rust APIs follow the release-stage semantic-versioning policy;
- serialized schema compatibility is versioned independently from crate semver;
- adapter updates may add provider or protocol capabilities without changing
  core types when they fit bounded extensions;
- changing an enum's wire representation, canonical form, ID format, trust
  semantics, error category, or budget accounting is a schema/RFC change;
- internal struct layout is not stable ABI and no C ABI is promised by v1.

## Alternatives considered

### Use `serde_json::Value` for all state and tool data

Rejected because it moves schema, trust, migration, redaction, and deterministic
merge failures to runtime and makes public guarantees difficult to enforce.
JSON remains available at explicit structured-content and erased-adapter
boundaries.

### Re-export one provider SDK's request/response types

Rejected because provider release cycles, authentication, role models, streaming
events, and extension semantics would become StateKnot's public compatibility
surface and would disadvantage other providers.

### Use MCP or A2A types as the common capability model

Rejected because MCP tools, A2A tasks, internal runs, local tools, and model
requests have different identity, lifecycle, durability, and authorization
semantics. Explicit adapters are safer and independently versionable.

### Expose only erased dynamic tools

Rejected because application authors would lose compile-time input/output types
and would repeatedly implement unsafe JSON decoding. A typed authoring trait plus
one framework-owned erased adapter provides both ergonomics and heterogeneity.

### Add a general service container to context

Rejected because it hides dependencies, enables accidental secret/database
access, complicates testing, and turns context into an unversioned plugin API.
Contexts expose a fixed least-privilege surface.

## Validation and rollout

Before this RFC can be accepted:

1. first-agent, typed-tool, model-stream, and protocol-adapter contract examples
   compile on MSRV without provider, Tokio, database, or server dependencies in
   `stateknot-core`;
2. every identifier, timestamp, money, digest, schema, content, descriptor,
   error, and durable envelope has canonical round-trip fixtures;
3. property tests cover constructor bounds, canonicalization stability, budget
   arithmetic, delegation scope intersection, and extension limits;
4. fuzz tests cover domain deserialization, schemas, canonical JSON, unknown
   fields, deeply nested content, oversized values, and malicious Unicode;
5. compile-fail tests prove that secret/context handles cannot be serialized and
   typed tools cannot enter the registry without valid schemas/descriptors;
6. N-1/N-2 fixture migrations demonstrate explicit unsupported-version and
   corruption behavior;
7. a dependency review confirms that core has no provider, protocol, database,
   HTTP server, async executor, or telemetry exporter dependency;
8. the three qualification scenarios map every required domain value to these
   types without an unbounded property bag;
9. a security review covers instruction promotion, descriptor substitution,
   cross-tenant IDs, extension smuggling, secret formatting, resource exhaustion,
   and ambiguous external outcomes.

The rollout order is core value types and fixtures, typed tool adapter, model
boundary, context/identity/budget integration, and only then graph/persistence/
protocol adapters. No crate is published while contract fixtures or materially
changing questions remain unresolved.

## Unresolved questions

1. Benchmark RFC 8785 canonicalization and schema validation against the GS-001
   event rate before accepting the implementation dependency; the canonical
   behavior remains required even if the implementation changes.
