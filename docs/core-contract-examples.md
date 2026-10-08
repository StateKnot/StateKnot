<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Public core contract examples

Status: implemented source evidence for validation gate 1 of
[RFC-0001](rfcs/0001-core-domain-and-capability-model.md). The RFC remains
Draft and the public API is not stable. The published `0.1.0-alpha.1` remains a
preview; this guide tracks current source evidence rather than release contents.

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
cargo test -p stateknot-core --test fixture_catalog --locked
PROPTEST_RNG_SEED=20261008 cargo test -p stateknot-core --test canonical_values --test value_properties --locked
cargo test -p stateknot-core -p stateknot-integrations -p stateknot --doc --locked
cargo test -p stateknot-runtime --test tool_registration --locked
```

CI has a named example-compilation step. The dependency-boundary integration
test also reads locked Cargo metadata and compares every direct normal and
development dependency with a reviewed allowlist. Adding or renaming a direct
dependency, including a target-specific dependency, fails the gate until the
core runtime-neutrality review is updated deliberately.

## Sealed compatibility fixture corpus

The versioned `catalog-v1.json` closes the inventory around all 39 currently
committed Core compatibility fixture documents. Every entry binds the exact file
bytes with SHA-256, including negative vectors that deliberately cannot be RFC
8785 canonicalized. The catalog root separately binds the ordered path, schema,
and content-digest records through a domain-separated RFC 8785 preimage.

The authorization slice freezes complete Tool receipts both with and without a
Skill acting-window binding, plus complete Skill approval, open-request,
acting-window, and revocation records. Executable vectors verify every retained
digest, payload redaction, closed schemas, strict source/duration decoding, and
fail-closed behavior after identity, input, expiry, reason, or digest tampering.

The integration gate rejects an unregistered, missing, reordered, duplicate,
oversized, duplicate-key, path-escaping, schema-conflicting, changed, or
unreferenced fixture. Every catalogued document must still be consumed by an
executable Rust compatibility test; a digest alone is not test coverage.

This makes the existing evidence corpus reviewable and tamper-evident. It does
not prove that every RFC-0001 public value and durable envelope already has a
fixture. Validation item 2 remains open until that type-level coverage audit and
the missing fixtures are complete.

### Value-level fixture and property coverage

[`canonical_values.rs`](../crates/stateknot-core/tests/canonical_values.rs) maps
38 public value types to positive and negative frozen vectors. Each positive
vector is decoded, serialized without changing its wire value, canonicalized,
and decoded again from the same RFC 8785 bytes; the digest must remain identical.
The tests use the actual public types, including all 18 macro-generated UUIDv7
identifiers. A source inventory guard fails if a generated identifier is added
without the corresponding typed fixture and property test.

| Public types | Frozen fixture section |
| --- | --- |
| `RunId`, `ThreadId`, `EventId`, `FailureId`, `MessageId`, `ArtifactId`, `AuthorizationReceiptId`, `SkillActivationApprovalId`, `SkillActingWindowId`, `InvocationId`, `InterruptId`, `TimerId`, `DeliveryId`, `DestinationId`, `CheckpointId`, `QuarantineId`, `AttemptId`, `SchedulerReservationId` | `core-identifiers-v1.json`: `uuid_v7` |
| `TenantId`, `SchedulerShardId`, `AgentSubmissionKey` | `core-identifiers-v1.json`: `tenants`, `shards`, `submission_keys` |
| `Version`, `Digest` | `core-scalars-v1.json`: `versions`, `digests` |
| `Timestamp`, `DurationMillis` | `core-time-v2.json`: `timestamps`, `durations_millis` |
| `TokenCount`, `ByteCount`, `ExecutionCount`, `CurrencyCode`, `Money` | `core-accounting-v1.json`: `counts`, `currencies`, `money` |
| `IssuerId`, `SubjectId`, `PrincipalIdentity` | `core-identity-v1.json`: `issuers`, `subjects`, `principal_identities` |
| `SchemaId`, `SchemaReference` | `core-schema-v1.json`: `ids`, `references` |
| `CapabilityName`, `Scope`, `ScopeSet` | `core-authorization-v1.json`: `capability_names`, `scopes`, `scope_sets` |

[`value_properties.rs`](../crates/stateknot-core/tests/value_properties.rs)
supplies independent models for the following RFC-0001 item 3 requirements.
Together with the 18 identifier properties, these 31 property tests each run
256 bounded generated cases. CI runs a fixed seed for reproduction; the full
workspace tests also execute the ordinary random-seed run.

| Requirement | Executable model and retained coverage |
| --- | --- |
| Constructor bounds | Identity ASCII/length grammars agree with constructors and Serde; UUID bits retain exact identity and reject every other version and variant class; versions/digests retain all integer/byte values; timestamps retain their full range and reject out-of-range values; durations reject numeric wire forms, overflow and precision loss; currencies require uppercase ASCII. Schema IDs require normalized HTTPS, while issuer identity deliberately preserves exact case. Existing content, descriptor and invocation-limit properties remain in their Core modules. |
| Canonicalization stability | Every listed value preserves wire bytes and digest through canonical decoding. Arbitrary Unicode object keys match an independent UTF-16 sort model, with a fixed supplementary-plane/private-use counterexample to Rust string ordering. Existing JSON, graph, checkpoint, barrier and recovery-order properties remain required. |
| Budget arithmetic | All three count types and Money match checked `u64` addition, subtraction and multiplication; cross-currency operations fail. Duration arithmetic matches nonnegative `i64`. Existing `budget.rs`, `budget_reservation_tests.rs` and `child_run_budget_tests.rs` retain narrowing, high-water, reservation-order and repeated-settlement models. |
| Delegation intersection | Caller, grant and policy scopes match the three-way bit-set intersection, remain associative and cannot widen any participant. The existing two-party commutativity/idempotence model remains required. |
| Extension limits | Complete-map bytes, per-key bytes and entry count accept the exact boundary and reject a one-unit narrowing; duplicate entries fail. The existing insertion-order/accounting property and deterministic nested-JSON hard-limit tests remain required. |

This closes the listed value-family gaps. Content, descriptors, errors, composite
durable envelopes, their nested identifiers and historical migration fixtures
still require the complete R1 type audit; C2/C3 and RFC-0001 remain open. These
tests do not establish production capacity, a fuzz qualification or a new release.

## What each example proves

| Example | Compiled contract | Explicit non-claim |
| --- | --- | --- |
| [`first_agent`](../crates/stateknot-core/examples/first_agent.rs) | Constructs immutable Agent and Model descriptors, schema-bound bounded input, restrictive request limits, and a complete finite resolved budget. | It does not admit a Run, contact a provider, or execute a Graph. |
| [`typed_tool`](../crates/stateknot-core/examples/typed_tool.rs) | Derives typed input/output schemas, canonicalizes and pins their digests, verifies the generated Rust schemas through an offline registry, and constructs the framework-owned `ToolAdapter`. | It does not execute a Tool attempt or authorize an external effect. The compact registry is example-local; production integrations use the immutable JSON Schema 2020-12 registry. |
| [`model_stream`](../crates/stateknot-core/examples/model_stream.rs) | Builds a finite streaming request and attempt context, checks model capabilities, then validates a contiguous Started → Output → Completed event sequence into a bounded `ModelResponse`. It also compiles the object-safe `Model::stream` entry. | It supplies no provider, executor, transport, credential, or durable attempt ledger. |
| [`protocol_adapter`](../crates/stateknot-core/examples/protocol_adapter.rs) | Parses a closed external request, assigns trusted schema identity locally, bounds caller-selected output bytes, resolves the Agent contract, and rejects injected authority fields. | It is not an HTTP, MCP, or A2A transport and creates no durable admission. |

## Compile-time privacy regression

The Core crate documentation has separate compile-fail checks for
`CancellationSignal`, `ModelContext`, `ToolContext`, and
`ToolReconciliationContext`: none may satisfy `serde::Serialize`. A passing
control checks all four public type names and their existing `Clone` contract,
and proves that a durable `BudgetUsage` record does satisfy the same Serde bound.
The Agent HTTP credential documentation separately checks that
`AgentHttpCredential` can be constructed and redacts Debug, while rejecting the
serialization bound. Removing an import or accidentally adding a serialization
implementation therefore cannot leave the evidence green.

The remaining guards cover `ToolIdempotencyKey`, all five first-party zeroizing
credential wrappers (`AgentHttpCredential`, `ClientSecret`, `ApiKey`, `A2aSecret`,
`McpServerBearerCredential`), and their static provider, MCP authorization, OAuth
registration and A2A security/push carriers. Construction controls exercise the
public imports and redacted Debug. The SDK's `McpOAuthStoredCredentials` and
`McpOAuthStoredAuthorizationState` intentionally remain serializable for
caller-owned encrypted storage; they must never enter ordinary run state or
audit payloads. See the [OAuth storage boundary](mcp-oauth.md).

Two complete Tool compile-fail implementations omit `JsonSchema` on input and
output separately; a successful implementation restores both derives. The
[production registry tests](../crates/stateknot-runtime/tests/tool_registration.rs)
exercise real directional Serde output, invalid meta-schema, non-object input,
missing/substituted pins and changed descriptors. Rejected dispatch paths assert
zero application calls. The adapter now generates the actual deserialization
input and serialization output contracts; the additive output registration
helper and compatibility rules are documented in
[RFC-0019](rfcs/0019-typed-tool-schema-directions.md) and the
[local Tool guide](local-tools.md).

CI has an explicit workspace Rustdoc-test step on Rust 1.88; `--all-targets`
tests and `cargo doc` alone do not execute these examples. These scoped checks
provide C5 evidence for the current listed types and typed adapter; they do not
prove arbitrary custom Serde implementations or complete every RFC acceptance
item. See the [R1 acceptance ledger](r1-contract-gap-ledger.zh-CN.md) (简体中文).

## Production integration boundary

These examples deliberately stop at core contract construction. A production
host must still authenticate and authorize the caller, resolve immutable
tenant-owned descriptors and schemas, commit durable admission, execute external
attempts through the invocation ledger, drive checkpoints under lease/fencing,
and revalidate terminal evidence before exposing a result. Use the
[typed Agent guide](typed-agent.md), [durable admission guide](durable-agent-admission.md),
and [durable Agent Loop guide](durable-agent-loop.md) for those implemented
boundaries.

Passing the four examples closes only RFC-0001 validation item 1. The sealed
fixture catalog is infrastructure toward item 2, not completion of its required
type-level coverage. Fuzzing, historical migrations, scenario mapping, and the complete security review also
remain acceptance gates. StateKnot remains a preview and RFC-0001 remains Draft.
