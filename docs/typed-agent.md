<!-- Copyright 2026 StateKnot contributors -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Typed Agent and first-party model adapters

This document describes the implemented pre-alpha boundary. It is deliberately
not a one-call `run()` guide: StateKnot does not let a convenience API bypass
durable admission, attempt ledgers, the Graph Driver, lifecycle evidence, or
tenant scheduling.

## What is implemented

`stateknot-runtime` now exposes:

- `AgentBuilder<I, O>`, which generates JSON Schema 2020-12 from the
  serialization contract of `I` and deserialization contract of `O`;
- RFC 8785 canonicalization and SHA-256 pinning of both generated documents;
- `TypedAgentDefinition<I, O>`, which registers the generated schema pair
  without exposing a partially registered builder;
- startup binding that requires every agent, tool, and model-profile schema to
  exist in one immutable offline registry and validates each model-visible
  schema against its pinned provider profile;
- `TypedAgent<I, O>::prepare_request`, which performs bounded JSON
  serialization and local input-schema validation before durable admission;
  and
- `TypedAgent<I, O>::decode_result`, which revalidates trusted provenance,
  request binding, complete budget/accounting evidence, and the output schema
  before deserializing `O`.

`stateknot-integrations` now exposes production-shaped bindings for:

- OpenAI Responses / OpenAI-compatible Responses endpoints; and
- Anthropic Messages endpoints.

Both adapters use attempt-scoped credentials, require HTTPS outside explicit
literal-loopback tests, disable HTTP redirects and hidden client retries, cap
request/response/SSE resources, honor cooperative cancellation and monotonic
deadlines, preserve provider request identifiers, normalize usage, and emit
only public-safe errors. API keys, provider bodies, prompt text, and model output
are not formatted into adapter diagnostics.

## Compile and run the local examples

The examples perform no provider request and require no credential:

```console
cargo run -p stateknot-runtime --example typed_agent
cargo run -p stateknot-integrations --example provider_adapters
```

They are also compiled by the workspace `--all-targets` CI gate. Read the exact
sources:

- [`crates/stateknot-runtime/examples/typed_agent.rs`](../crates/stateknot-runtime/examples/typed_agent.rs)
- [`crates/stateknot-integrations/examples/provider_adapters.rs`](../crates/stateknot-integrations/examples/provider_adapters.rs)

The typed flow is intentionally explicit:

```rust,ignore
let definition = AgentBuilder::<IncidentRequest, IncidentReport>::new(
    agent_metadata,
    input_schema_id,
    output_schema_id,
    model_descriptor,
    trusted_instructions,
    execution_limits,
)
.build()?;

let mut schemas = JsonSchemaRegistryBuilder::default();
schemas.register(provider_profile, provider_profile_document)?;
let schemas = definition.register_schemas(schemas)?.build()?;
let agent = definition.bind(Arc::new(schemas))?;

let request = agent.prepare_request(&input, request_budget_limits)?;
// Durable admission assigns tenant/run/thread/invocation identity and freezes
// ResolvedBudget before DurableAgentLoop executes the claimed graph.
```

`register_schemas` moves the startup builder in and returns it only if both
generated resources were accepted. On failure there is no caller-visible
half-installed input/output pair.

## Provider contract

| Boundary | OpenAI Responses | Anthropic Messages |
| --- | --- | --- |
| Complete response | Implemented | Implemented |
| True incremental SSE | Implemented | Implemented |
| Text input/output | Implemented | Implemented |
| JSON Schema output | Implemented | Implemented |
| Function/tool proposals | Implemented | Implemented |
| Local argument/output validation | Implemented | Implemented |
| Provider-native unary tool continuation | Implemented | Implemented |
| Generic JSON mode | Implemented | Rejected: stable schema-constrained output is required |
| Readable reasoning summaries | Implemented when declared by the binding | Not advertised by this adapter version |
| Legacy `role=tool` messages | Rejected before I/O | Rejected before I/O |
| Artifact/multimodal input | Rejected before I/O | Rejected before I/O |
| Request extensions | Rejected before I/O | Rejected before I/O |

`ModelTranscript` is the lossless continuation contract. Each turn binds one
normalized `ModelResponse`, its exact bounded provider replay fragment, and one
committed `ModelToolOutcome` per proposal in provider order. Construction and
deserialization reject missing or reused attempt/call/invocation identities,
tool-version drift, ambiguous external effects, payload substitution, and
resource-limit violations. Before network I/O, the selected adapter verifies
the model/provider binding and replay format, reparses the opaque fragment, and
requires it to match the normalized response exactly.

OpenAI replays the complete prior `response.output` before corresponding
`function_call_output` items and requests encrypted reasoning continuation when
tools are enabled. Anthropic replays the complete assistant content, followed
immediately by one user message containing all ordered `tool_result` blocks.
Plain `role=tool` messages remain rejected because they cannot prove either
provider-native ordering contract. Tool-producing streaming attempts do not yet
emit replay evidence; the durable Agent tool loop must use complete responses
until the streaming event contract carries an exact continuation snapshot.

Streaming adapters do not buffer a complete response and replay fake chunks.
They incrementally parse bounded SSE, validate every emitted event with the core
`ModelEventAccumulator`, apply bounded channel backpressure, and require a
successful terminal event. OpenAI's terminal response snapshot is cross-checked
against every streamed semantic item. Truncated, duplicated, reordered, or
substituted terminal data fails without emitting `Completed`.

## Durable execution boundary

`TypedAgent` is a typed contract and codec, not a process-local runner. The
supported path remains:

1. authenticate and select immutable agent/model/tool descriptor snapshots;
2. validate the typed request and all digest-pinned schemas offline;
3. resolve system, tenant, policy, agent, and request limits into one finite
   `ResolvedBudget`, then commit the immutable intent, initial event, and
   superstep-zero checkpoint through `DurableAgentAdmission`;
4. claim the graph with a lease/fence;
5. execute model and tool attempts through `DurableInvocationExecutor`, which
   commits attempt start before external dispatch;
6. drive checkpoints and lifecycle handoffs through `DurableAgentLoop`; and
7. validate and decode the terminal `AgentResult` through `TypedAgent`.

Atomic durable admission is implemented; see
[`durable-agent-admission.md`](durable-agent-admission.md). Durable ingress-key
mapping and fully revalidated run/result reads are implemented by
`DurableAgentRuns`; see [`durable-agent-runs.md`](durable-agent-runs.md). There
is still no helper that synthesizes and executes the prebuilt provider-native
model/tool graph in one call, and no temporary in-memory `run()` method stands
in for that missing orchestration.

## Verification evidence

The adapter suite uses a real local TCP HTTP server rather than a mocked SDK. It
currently covers request headers and bodies, usage normalization, fragmented
SSE, backpressure-compatible incremental events, terminal cross-checking,
truncation, duplicate JSON members, response substitution, 429 `Retry-After`,
5xx no-hidden-retry behavior, request/response byte ceilings, credential
deadline, cancellation precedence, and secret/debug redaction.

Run it with:

```console
cargo test -p stateknot-integrations --all-targets
cargo test -p stateknot-runtime --test typed_agent
```

Live-provider qualification, provider drift cassettes, durable transcript
assembly inside the prebuilt Agent graph, policy middleware, and cancellation
service integration remain release gates. These adapters and typed
APIs are implemented but still pre-alpha and unpublished.
