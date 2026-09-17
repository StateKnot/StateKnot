<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Public core contract examples

Status: implemented pre-alpha evidence for validation gate 1 of
[RFC-0001](rfcs/0001-core-domain-and-capability-model.md). The RFC remains
Draft and the public API is not stable or published.

The four `stateknot-core` examples compile and run without a model-provider SDK,
Tokio, a database, an HTTP server, or a protocol SDK. They exercise the real
bounded constructors and fail-closed validation paths instead of pseudocode.

## Run the evidence

```console
cargo check -p stateknot-core --examples --locked
cargo run -p stateknot-core --example first_agent --locked
cargo run -p stateknot-core --example typed_tool --locked
cargo run -p stateknot-core --example model_stream --locked
cargo run -p stateknot-core --example protocol_adapter --locked
cargo test -p stateknot-core --test dependency_boundary --locked
```

CI has a named example-compilation step. The dependency-boundary integration
test also reads locked Cargo metadata and compares every direct normal and
development dependency with a reviewed allowlist. Adding or renaming a direct
dependency, including a target-specific dependency, fails the gate until the
core runtime-neutrality review is updated deliberately.

## What each example proves

| Example | Compiled contract | Explicit non-claim |
| --- | --- | --- |
| [`first_agent`](../crates/stateknot-core/examples/first_agent.rs) | Constructs immutable Agent and Model descriptors, schema-bound bounded input, restrictive request limits, and a complete finite resolved budget. | It does not admit a Run, contact a provider, or execute a Graph. |
| [`typed_tool`](../crates/stateknot-core/examples/typed_tool.rs) | Derives typed input/output schemas, canonicalizes and pins their digests, verifies the generated Rust schemas through an offline registry, and constructs the framework-owned `ToolAdapter`. | It does not execute a Tool attempt or authorize an external effect. The compact registry is example-local; production integrations use the immutable JSON Schema 2020-12 registry. |
| [`model_stream`](../crates/stateknot-core/examples/model_stream.rs) | Builds a finite streaming request and attempt context, checks model capabilities, then validates a contiguous Started → Output → Completed event sequence into a bounded `ModelResponse`. It also compiles the object-safe `Model::stream` entry. | It supplies no provider, executor, transport, credential, or durable attempt ledger. |
| [`protocol_adapter`](../crates/stateknot-core/examples/protocol_adapter.rs) | Parses a closed external request, assigns trusted schema identity locally, bounds caller-selected output bytes, resolves the Agent contract, and rejects injected authority fields. | It is not an HTTP, MCP, or A2A transport and creates no durable admission. |

## Production integration boundary

These examples deliberately stop at core contract construction. A production
host must still authenticate and authorize the caller, resolve immutable
tenant-owned descriptors and schemas, commit durable admission, execute external
attempts through the invocation ledger, drive checkpoints under lease/fencing,
and revalidate terminal evidence before exposing a result. Use the
[typed Agent guide](typed-agent.md), [durable admission guide](durable-agent-admission.md),
and [durable Agent Loop guide](durable-agent-loop.md) for those implemented
boundaries.

Passing this evidence closes only RFC-0001 validation item 1. Canonical fixture
coverage, fuzzing, compile-fail privacy checks, historical migrations, scenario
mapping, and the complete security review remain acceptance gates. StateKnot
therefore remains pre-alpha and RFC-0001 remains Draft.
