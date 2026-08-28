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
capabilities, identity, budgets, execution context, errors, and the typed/erased
tool boundary.

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
use std::time::Duration;

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

struct RestartService;

impl Tool for RestartService {
    type Input = RestartInput;
    type Output = RestartOutput;

    fn descriptor(&self) -> Result<ToolDescriptor, DescriptorError> {
        ToolDescriptor::builder("ops.restart-service", Version::new(1, 0, 0))
            .description("Restart one allowlisted service deployment")
            .risk(ToolRisk::IdempotentWrite)
            .required_scope("ops:restart")
            .timeout_ceiling(Duration::from_secs(30))
            .build()
    }

    fn call<'a>(
        &'a self,
        ctx: ToolContext,
        input: Self::Input,
    ) -> BoxFuture<'a, Result<Self::Output, ToolError>> {
        Box::pin(async move {
            let key = ctx.required_idempotency_key()?;
            let credential = ctx.credentials().resolve("deployment-api").await?;
            restart_with_key(credential, key, input).await
        })
    }
}
# fn restart_with_key(
#     _: ResolvedCredential,
#     _: IdempotencyKey,
#     input: RestartInput,
# ) -> BoxFuture<'static, Result<RestartOutput, ToolError>> {
#     Box::pin(async move {
#         Ok(RestartOutput {
#             deployment_id: String::from("deployment-42"),
#             accepted_revision: input.expected_revision,
#         })
#     })
# }
# fn main() {}
```

The builder is fallible because capability names, versions, scopes, descriptions,
timeouts, schemas, and extension sizes are validated. The typed tool is wrapped
by an erased adapter only after both generated schemas pass validation.

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
) -> Result<IncidentSummary, Box<dyn std::error::Error>> {
    let request = ModelRequest::builder()
        .instruction(Instruction::trusted("Return a typed incident summary")?)
        .message(Message::user([ContentPart::text("Investigate incident 42")?])?)
        .required(ModelRequirement::ToolCalling)
        .required(ModelRequirement::StructuredOutput)
        .output_schema(JsonSchemaDocument::for_type::<IncidentSummary>()?)
        .build()?;

    model.capabilities().satisfies(request.requirements())?;
    let response = model.invoke(ctx, request).await?;
    Ok(response.decode_structured()?)
}
```

Capability mismatch fails before provider invocation. Provider-specific options
may be supplied through a registered, namespaced extension, but cannot weaken
budgets, policy, schema validation, or durable guarantees.

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

`CapabilityDescriptor` is the common discovery record for a model, tool, agent,
or workflow capability. It contains a validated name, version, description,
owner/provenance, supported modalities, schemas, required scopes, risk metadata,
limits, and namespaced extensions.

It is not itself executable. Execution uses the corresponding model, tool, or
agent trait so unlike operations do not pretend to share one lifecycle.

### Model capabilities

`ModelCapabilities` explicitly records:

- supported input and output modalities;
- streaming support;
- tool-calling and parallel-tool-calling support;
- structured-output support and accepted schema subset;
- known context/output token limits;
- provider features represented by registered extension keys.

`ModelRequirement` records the capabilities a request needs. Negotiation returns
a `CapabilityMismatch` listing every unmet requirement. A runtime snapshots the
negotiated capabilities with the attempt so recovery can detect adapter or
provider drift.

Capability data is evidence from an adapter and may become stale. It does not
override provider errors, tenant policy, or configured safety limits.

### Tool capabilities

`ToolDescriptor` contains:

- stable capability name and semantic version;
- input and output schemas;
- `ToolRisk::{ReadOnly, IdempotentWrite, NonIdempotentWrite}`;
- required scopes and approval policy reference;
- idempotency support and optional status-query/compensation capability;
- timeout ceiling, concurrency class, and maximum result/artifact sizes;
- provenance and bounded namespaced extensions.

`ReadOnly` is a security and semantic assertion by the tool owner, not an
inference from an HTTP method. Misdeclaring it is a tool defect detectable by
review and integration tests.

## Typed and erased tool boundary

The public authoring trait is typed:

```rust
pub trait Tool: Send + Sync + 'static {
    type Input: serde::de::DeserializeOwned + schemars::JsonSchema + Send + 'static;
    type Output: serde::Serialize + schemars::JsonSchema + Send + 'static;

    fn descriptor(&self) -> Result<ToolDescriptor, DescriptorError>;

    fn call<'a>(
        &'a self,
        context: ToolContext,
        input: Self::Input,
    ) -> BoxFuture<'a, Result<Self::Output, ToolError>>;
}
```

StateKnot owns an object-safe erased adapter used by registries and the runtime.
The erased interface is not the recommended application-authoring API. It:

1. validates canonical JSON input against the descriptor schema;
2. deserializes into `Tool::Input`;
3. invokes the typed implementation;
4. serializes and validates `Tool::Output`;
5. returns a bounded `ToolResult` with artifacts and external references.

No tool receives a raw provider request, database transaction, bearer token, or
unrestricted service locator.

## Model boundary

The object-safe `Model` trait exposes descriptor/capabilities plus unary and
streaming invocation through runtime-neutral futures and streams:

```rust
pub trait Model: Send + Sync + 'static {
    fn descriptor(&self) -> &ModelDescriptor;
    fn capabilities(&self) -> &ModelCapabilities;

    fn invoke<'a>(
        &'a self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'a, Result<ModelResponse, ModelError>>;

    fn stream<'a>(
        &'a self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxStream<'a, Result<ModelEvent, ModelError>>;
}
```

`ModelRequest` contains ordered instructions, messages, available tool
descriptors, required capabilities, output schema, sampling/limit values, and a
bounded extension map. It does not contain a provider SDK request object.

`ModelResponse` contains typed content, validated tool-call proposals, finish
reason, usage, adapter/provider identifiers, and redacted provider metadata.
Tool-call proposals are not invocations until StateKnot validates schema,
policy, budget, approval, and invocation-ledger state.

Streaming emits semantic `ModelEvent` values. Adapters may coalesce provider
deltas, but must produce one validated terminal response or one error. Partial
content is never treated as a committed complete response.

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
