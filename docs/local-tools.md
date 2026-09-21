<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Register local Rust tools

[简体中文](local-tools.zh-CN.md)

A local Tool is ordinary application code behind StateKnot's typed, immutable,
durable execution boundary. The production path is:

1. implement `stateknot_core::Tool` for one exact behavior version;
2. generate and pin its input and output schemas in `JsonSchemaRegistryBuilder`;
3. construct an exact `ToolDescriptor` and `ToolAdapter`;
4. freeze the adapter in `ToolProviderRegistryBuilder`; and
5. expose the same descriptor through the Agent's `AgentTools`.

Run the complete startup example:

```console
cargo run -p stateknot-runtime --example local_tool_registration --locked
```

The [source](../crates/stateknot-runtime/examples/local_tool_registration.rs)
performs no Tool call or external I/O. It proves that generated Rust schemas,
the descriptor, the executable adapter, and the Agent-visible declaration all
name the same immutable revision.

## Define a closed typed boundary

```rust,ignore
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncidentLookup {
    incident_id: String,
}

#[derive(JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct IncidentSummary {
    found: bool,
    severity: String,
}

impl Tool for IncidentLookupTool {
    type Input = IncidentLookup;
    type Output = IncidentSummary;

    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(
        &self,
        context: ToolContext,
        input: Self::Input,
    ) -> BoxFuture<'_, Result<ToolOutput<Self::Output>, ToolError>> {
        // Exactly one admitted physical attempt. Use `context` for the durable
        // invocation/attempt identity, cancellation, deadlines and evidence.
        # todo!()
    }
}
```

Use object-root inputs, closed Serde decoding, bounded fields, and explicit
outputs. Never accept an unbounded `serde_json::Value` merely to avoid defining
a contract. `ToolAdapter` validates the generated input schema at startup,
validates bounded input before application code, serializes the typed result,
then validates the output against the same frozen schema registry.

`call` represents exactly one physical attempt. It must not hide retries. Each
external exchange needs separately admitted durable attempt evidence. A returned
`ToolError` must report truthful effect evidence: after a write has possibly
reached the target, never describe the outcome as safely unapplied. Observe the
cooperative cancellation and deadline in `ToolContext`, but do not claim that a
cancelled future recalled an already issued external write.

## Generate and pin schemas at startup

```rust,ignore
let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
let input = schemas.register_rust_type::<IncidentLookup>(
    "https://schemas.example.com/tools/lookup/input/1.0.0".parse()?,
    Version::new(1, 0, 0),
)?;
let output = schemas.register_rust_type::<IncidentSummary>(
    "https://schemas.example.com/tools/lookup/output/1.0.0".parse()?,
    Version::new(1, 0, 0),
)?;
let schemas = schemas.build()?;
```

`register_rust_type` inserts the canonical `$id`, computes the RFC 8785
SHA-256 digest, returns the complete `SchemaReference`, and registers an offline
JSON Schema 2020-12 validator. `ToolAdapter::new` later regenerates the Rust
schema and requires exact canonical equality. A stale descriptor, changed Rust
type, missing local resource, unresolved `$ref`, or digest mismatch fails while
building the process snapshot, before any run can dispatch.

Treat these schema URIs and versions as released interfaces. A compatible
source refactor that produces identical schema bytes can retain the version.
Any wire-shape or behavior change receives new schema and Tool versions.

## Describe effects and resource ceilings accurately

The descriptor is not display metadata. It controls admission, scheduling,
recovery and policy. Declare:

- the exact owner, registry-local name and semantic version;
- input and output `SchemaReference` values returned above;
- `ToolRisk`, idempotency, replay and status-query semantics;
- cancellation and progress capabilities; and
- finite time, concurrency, input, output, progress and artifact limits.

A descriptor that advertises reconciliation must be backed by an adapter that
can query authoritative provider state, or by a separately authorized manual
reconciler. Do not model a non-idempotent write as read-only to obtain parallel
execution. Resource requirements are policy inputs, not permission grants.

## Freeze the executable and Agent views together

```rust,ignore
let descriptor = build_descriptor(input, output)?;
let adapter = ToolAdapter::new(
    IncidentLookupTool { descriptor: descriptor.clone() },
    schemas.clone(),
)?;

let mut providers = ToolProviderRegistryBuilder::new();
providers.register(Arc::new(adapter))?;
let providers = providers.build();

let agent_tools = AgentTools::try_new([descriptor.clone()])?;
assert_eq!(providers.resolve(&descriptor)?.descriptor(), &descriptor);
```

`AgentTools` is the canonical, model-visible declaration. The provider registry
is the executable worker snapshot. Registration rejects duplicate exact
identities and inconsistent reconciliation claims. Resolution requires both the
same owner/name/version identity and byte-for-byte descriptor equality; there
is no alias, priority or fallback selection during recovery.

Build a new immutable deployment snapshot to enable, disable or upgrade a Tool.
Do not mutate a live registry. Retain old executable revisions until all runs
that durably reference them have drained.

## Keep durable execution outside Tool code

Do not invoke the adapter directly from an Agent node. `DurableInvocationExecutor`
owns the production sequence:

1. validate budget, policy, descriptor and bounded input;
2. commit an exact attempt start in PostgreSQL;
3. resolve the exact descriptor from the immutable registry;
4. perform one provider call without holding a database transaction; and
5. fence and commit terminal result or failure evidence.

If the process dies after dispatch but before terminal commit, recovery uses
the descriptor's declared idempotency and reconciliation semantics. Unknown
effect evidence never becomes an automatic duplicate write. See
[Durable model and Tool invocation execution](durable-invocation-executor.md)
for handoffs, reconciliation and lost-acknowledgement behavior.

## Mix local, MCP and A2A providers without precedence

Local `ToolAdapter`, `McpRemoteTool`, `McpSkillBoundTool`, and `A2aRemoteAgent`
all implement `ErasedTool`. Register any of them in the same
`ToolProviderRegistryBuilder` as `Arc<dyn ErasedTool>`. Their protocol changes
transport behavior, not durable identity or execution rules.

Exact identity collisions fail startup. StateKnot never lets a remote discovery
result silently shadow local code, and it never falls back from a missing local
revision to a similarly named remote capability. Resolve discovery, policy and
version selection before admitting the Agent, then freeze the result. For
remote Skill-specific approval, use the guarded profile in
[MCP Skills client and Host](mcp-skills-host.md).

## Production checklist

- Pin one immutable descriptor and schema pair per executable behavior.
- Deny unknown input fields and bound every potentially large value.
- Make side effects, idempotency, cancellation and reconciliation truthful.
- Use credentials and clients captured by the Tool implementation; never put
  secrets in descriptors, errors, events or model-visible output.
- Keep one call to one physical attempt; let the durable runtime own retries.
- Register every Agent-visible descriptor in the same Worker snapshot.
- Run integration tests with real dependencies, process kill, lost response,
  cancellation and lease takeover before claiming production qualification.
