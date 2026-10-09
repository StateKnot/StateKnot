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
cargo test -p stateknot-core --test public_type_inventory --locked
cargo test -p stateknot-core --test public_enum_variants --locked
cargo test -p stateknot-core --test canonical_execution_wires --locked
cargo test -p stateknot-core --test canonical_time --locked
PROPTEST_RNG_SEED=20261008 cargo test -p stateknot-core --test canonical_values --test value_properties --test nested_json_properties --locked
cargo test -p stateknot-core -p stateknot-integrations -p stateknot --doc --locked
cargo test -p stateknot-runtime --test tool_registration --locked
```

CI has a named example-compilation step. The dependency-boundary integration
test also reads locked Cargo metadata and compares every direct normal and
development dependency with a reviewed allowlist. Adding or renaming a direct
dependency, including a target-specific dependency, fails the gate until the
core runtime-neutrality review is updated deliberately.

## Sealed compatibility fixture corpus

The versioned `catalog-v1.json` closes the inventory around all 44 currently
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

This makes the existing evidence corpus reviewable and tamper-evident. It
now includes the typed root-export audit below. The catalog count itself is
not a coverage metric; variant combinations, property/fuzz and historical
qualification remain separate gates.

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

[`value_properties.rs`](../crates/stateknot-core/tests/value_properties.rs) and
[`nested_json_properties.rs`](../crates/stateknot-core/tests/nested_json_properties.rs)
supply independent models for the following RFC-0001 item 3 requirements.
Together with the 18 identifier properties, these 35 property tests each run
256 bounded generated cases. CI runs a fixed seed for reproduction; the full
workspace tests also execute the ordinary random-seed run.

| Requirement | Executable model and retained coverage |
| --- | --- |
| Constructor bounds | Identity ASCII/length grammars agree with constructors and Serde; UUID bits retain exact identity and reject every other version and variant class; versions/digests retain all integer/byte values; timestamps retain their full range and reject out-of-range values; durations reject numeric wire forms, overflow and precision loss; currencies require uppercase ASCII. Schema IDs require normalized HTTPS, while issuer identity deliberately preserves exact case. Existing content, descriptor and invocation-limit properties remain in their Core modules. |
| Canonicalization stability | Every listed value preserves wire bytes and digest through canonical decoding. Arbitrary Unicode object keys match an independent UTF-16 sort model, with a fixed supplementary-plane/private-use counterexample to Rust string ordering. Nested arrays/objects with Unicode keys/values and safe integer leaves match a separate recursive canonical-byte model and SHA-256. Existing JSON, graph, checkpoint, barrier and recovery-order properties remain required. |
| Budget arithmetic | All three count types and Money match checked `u64` addition, subtraction and multiplication; cross-currency operations fail. Duration arithmetic matches nonnegative `i64`. Existing `budget.rs`, `budget_reservation_tests.rs` and `child_run_budget_tests.rs` retain narrowing, high-water, reservation-order and repeated-settlement models. |
| Delegation intersection | Caller, grant and policy scopes match the three-way bit-set intersection, remain associative and cannot widen any participant. The existing two-party commutativity/idempotence model remains required. |
| Extension limits | Complete-map bytes, per-key bytes and entry count accept the exact boundary and reject a one-unit narrowing; duplicate entries fail. Independent nested-tree accounting checks compact bytes, depth, container entries, value nodes excluding keys, decoded string bytes and decoded key bytes. Both raw/materialized constructors agree with narrowed profiles; exact limits pass, one-unit tightenings fail, and trailing whitespace hits the raw byte gate. Opaque/schema-bound extension construction and restriction enforce the same per-value limits on previously wider values. The existing insertion-order/accounting property and deterministic hard-limit tests remain required. |

The value-family evidence remains required. The root-export inventory below
adds every current serializable public type; variant combinations, nested
properties and historical migrations remain open acceptance work. These
tests do not establish production capacity, a fuzz qualification or a new release.

[`canonical_time.rs`](../crates/stateknot-core/tests/canonical_time.rs) also checks
all 2,360 ASCII non-digit substitutions across the timestamp's 20 digit positions
and Unicode replacements that preserve its byte length and separators. Direct
parsing, JSON text/value readers and the retained nested Run transition reject
malformed timestamps. Digit validation precedes arithmetic in every build
profile; valid wire bytes, ranges and schema pins remain fixed.

### Complete execution wire coverage

[`canonical_execution_wires.rs`](../crates/stateknot-core/tests/canonical_execution_wires.rs)
provides a typed matrix for 67 more public types: 62 closed objects/tagged
variants and five identity collections. Its explicit JSON pointers map 104
positive vectors in `core-execution-wires-v1.json` to the actual public readers.
Every vector preserves its wire value, RFC 8785 bytes and digest on canonical
round-trip. Object vectors reject wrong shapes, unknown authority fields and
raw duplicate keys; 32 explicitly selected checked digest fields also reject
substitution and omission. Collection vectors reject wrong shapes and duplicate
identities, and freeze whether an empty batch is allowed.

| Public types | Complete fixture family |
| --- | --- |
| `CompiledGraph`, `GraphExecutionLimits`, `GraphNode`, `GraphReducerReference`, `GraphRoute`, `GraphRoutes`, `ReadyNodes` | `graph` |
| `CheckpointHead`, `CheckpointState`, `CheckpointWrite`, `GraphReference`, `CheckpointBarrier`, `BarrierResultHeads` | `barrier` |
| `NodeActivation`, `RunFence`, `JournalHead`, `NodeAttempt`, `NodeAttemptStart`, `NodeAttemptStartHead`, `NodeAttemptCompletion`, `NodeAttemptOutcome` | `node_attempt`, `node_result` |
| `NodeControl`, `NodeStateChange`, `NodeStateUpdate`, `NodeTerminalOutput`, `NodeWait`, `NodeWaits`, `NodeInvocationBinding`, `NodeInvocationBindings`, `PendingNodeResult`, `PendingNodeResultHead`, `PendingNodeResultIntent` | `node_result`, `durable_wait`, `model_invocation` |
| `DeliveryFence`, `OutboxDestinationRef`, `OutboxDelivery`, `OutboxDeliveryIntent`, `OutboxDeliveryHead`, `OutboxAttempt`, `OutboxAttemptStart`, `OutboxAttemptStartHead`, `OutboxAttemptCompletion`, `OutboxAttemptOutcome` | `outbox` |
| `InterruptRecord`, `InterruptRequest`, `InterruptRequestHead`, `InterruptRequestIntent`, `InterruptResolution`, `InterruptResolutionIntent`, `InterruptResolver`, `DurableTimer`, `DurableTimerHead`, `DurableTimerRecord`, `TimerFiring`, `TimerFiringIntent`, `TimerRegistrationIntent`, `WaitRegistrationIntent`, `DurableWait` | `durable_wait` |
| `ModelInvocation`, `ModelInvocationIntent`, `ModelInvocationHead`, `ModelInvocationState`, `ModelInvocationTransition` | `model_invocation` |
| `ToolInvocation`, `ToolInvocationIntent`, `ToolInvocationHead`, `ToolInvocationState`, `ToolInvocationTransition` | `tool_invocation` |

Empty tagged unit variants now reject additional fields through a shared
closed-object reader. This repairs the declared fail-closed contract for node
control/state changes, prepared model/tool states, journal source/expectation
and the pending Run state. Valid wire bytes, Rust variants and generated schema
pins remain unchanged. Previously swallowed extra fields are invalid input;
this change does not rewrite persisted records or bless them as historical
compatibility vectors. Strict RetryAdvice decoding remains a regression control.

The eight original family suites compare their constructors and complete
histories with this new document while retaining their earlier frozen digest
checks. Those 39 earlier documents retain their exact bytes. The vectors include
unfinished/succeeded/failed node and outbox attempts, unresolved/resolved
interrupts, unfired/fired timers, all four node control alternatives, both
model/tool bindings, and model retry and committed-tool histories.

A reference-only digest cannot always be verified from that type alone: schema
pins, external destination snapshots and some invocation heads need trusted
registry/history context. The matrix only demands local checksum rejection for
the explicitly selected fields; it preserves the existing context-bound
integrity and dispatch tests. This is current-source fixture evidence, not an
N-1/N-2 migration corpus or a production load result. Remaining public families,
variant combinations and the complete C2/C3 audit stay open.

### Closed public type and schema inventory

[`public_type_inventory.rs`](../crates/stateknot-core/tests/public_type_inventory.rs)
checks all 570 named root exports against a
[machine-readable inventory](../crates/stateknot-core/tests/fixtures/core-public-type-inventory-v1.json):
555 types, 11 traits and four constants. The compiler verifies 307 types with
`Serialize` and `DeserializeOwned`, two output-only types, and 246 reviewed
Rust-only instantiations without `Serialize` or `DeserializeOwned`. Each reader has an explicit fixture
file/JSON pointer, a canonical wire digest and a generated JSON Schema digest;
`BudgetRemaining` supplies the 308th schema pin. A separate output inventory
pins all 308 serialization profiles without changing the existing input pins. The 312-test matrix rejects
unsupported scalar/collection shapes, unknown fields on 181 closed object
vectors, and raw duplicate known keys. Bounded JSON and extension maps retain
open-key semantics. New exports, missing typed evidence, and accidental Serde
implementations fail CI until explicitly reviewed.

[RFC-0021](rfcs/0021-core-object-readers.md) requires the published object shape
at all 181 closed object readers. A streaming map guard preserves their owned
field readers, duplicate/unknown-field checks and constructors without an extra
JSON tree. The matrix rejects declaration-ordered positional arrays, including
empty, truncated and extended forms, through both text and Value readers.
Nested schema references in Tool, Agent, journal, checkpoint, child admission
and Join records have direct regressions. The actual Tool registry rejects
positional nested input before an application call and still dispatches valid
objects with exact output pins.

Use the object form specified by the immutable schema. Previously accepted
positional JSON is rejected; there is no data backfill or pin rewrite. These
objects support their canonical JSON contract, not sequence-based binary-format
decoding. Scalar, typed collection, BoundedJson and open extension shapes retain
their existing contracts. All 43 previous fixture documents, canonical wire digests and
both sets of 308 schema pins stay exact. This correction is source-only and does
not modify the published alpha.1 package.

`core-admission-transcript-wires-v1.json` supplies complete admission, reservation,
child accounting/Join, provider replay/tool outcome, Run lifecycle and composition
source wires. Six existing constructor families reproduce the complete values;
the previous 40 documents retain their exact bytes. `BudgetRemaining` and
`GraphNodeSource` remain output-only: their producers are checked, and compilation
rejects adding an owned reader without review. Generic Rust-only guards use the
explicit representative instantiations recorded in the manifest; they do not
prove the absence of every possible future conditional generic implementation.

This closes the current root-type inventory gap. Selected vectors are not an
exhaustive enumeration of variant combinations or historical versions. C2
optional/nested combination review, C3 composite/property auditing and C6 actual N-1/N-2 qualification
remain separate acceptance work. The bounded C4 gate below runs independently;
RFC-0001 remains Draft.

### Closed public enum alternatives

[`public_enum_variants.rs`](../crates/stateknot-core/tests/public_enum_variants.rs)
adds 73 checks for all 71 public reader/writer enums and their 298 serialized
alternatives. Its typed list must match the root-export inventory; each type's
fixed cases must match its generated schema alternatives with no duplicates.
Every case preserves producer bytes, canonical round trips and its frozen digest.
Object cases reject declaration-ordered arrays, unknown fields and every raw
duplicate known key. The existing root matrix retains its shape and pin checks.

[`core-public-enum-variants-v1.json`](../crates/stateknot-core/tests/fixtures/core-public-enum-variants-v1.json)
reuses independently valid sub-values from the current corpus and adds 28
public-constructor cases for missing alternatives, including lifecycle,
reconciliation, model capability and outbox branches. A source audit confirms
298 Rust enum variants and 298 schema alternatives. Optional fields are stored
in the actual producer form; an enclosing negative fixture does not make every
one of its nested values invalid. The previous 43 documents remain byte-exact;
only the catalog metadata adds this 44th document and binds its reviewed bytes.

The fuzz regression and seed generator exercise every alternative through the
actual input and output schema oracles. These are current-source vectors, not
historical releases or exhaustive combinations of optional and nested values.
Remaining composite properties, history and scenario qualification keep their
separate gates.

## Bounded fuzz and output-schema qualification

[`fuzz/qualify.py`](../fuzz/qualify.py) runs three ASan/libFuzzer targets: strict
bounded JSON/JCS, all 307 typed readers, and the actual offline runtime schema
registry. Accepted reader inputs are checked against their offline input schema
after actual deserialization, so an alternate accepted representation cannot be
masked by validating first. Producers retain their output-schema oracle.
It explicitly replays every seed before fixed-seed mutation, preserves
failure bytes and coverage cases, and records source/dependency/compiler hashes.
Every target has a 128 KiB input bound, 10-second per-input deadline, 2,048 MiB
RSS ceiling, and a mutation limit of 10,000 executions or 60 seconds. The runner
owns process groups, isolates each run's corpus, and rejects lock/source changes.
The nightly/compiler and dev-only engine are pinned separately from product
Rust 1.88 and dependencies. No integrity checks are disabled via `cfg(fuzzing)`.

Reproduction, retained malformed Unicode/duplicate/deep/oversized inputs and
artifact handling are documented in the [fuzz guide](../fuzz/README.md). The
`Core bounded fuzz` CI gate must pass on the final immutable source. This finite
profile does not establish exhaustive coverage, historical migration, production
capacity or independent security acceptance.

[RFC-0020](rfcs/0020-core-optional-output-schemas.md) corrects optional producer
schemas for `Failure`, `ToolError` and Capability lifecycle variants, reusing
actual borrowed serializer wires. Existing input schema/wire pins stay exact;
all 308 output pins are separately frozen. Actual parent-source output schemas
prove stale-pin startup rejection with zero calls through the real Tool adapter.
Changed output bytes require new schema/Tool versions and the old executable
registry for previously admitted work; durable pins are never rewritten.

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
