<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0019: Directional typed Tool schema registration

- Status: Accepted
- Authors: StateKnot contributors
- Created: 2026-10-08
- Tracking: [R1 acceptance ledger, C5](../r1-contract-gap-ledger.zh-CN.md)
- Supersedes: None
- Superseded by: None

## Summary and motivation

Typed Tool registration must pin the actual Serde input and output contracts.
`ToolAdapter` currently generates both with Schemars' default deserialization
profile. An output with directional `rename`, `skip_serializing`, optional fields
or defaults can pass startup checks against its input shape and fail only after
application code has run. For writes, that failure is already an applied effect.

Use explicit JSON Schema 2020-12 deserialization for `Tool::Input` and
serialization for `Tool::Output`. Add
`JsonSchemaRegistryBuilder::register_rust_output_type<T: JsonSchema + Serialize>`
to generate, canonicalize, pin and register the output profile. The existing
`register_rust_type<T: JsonSchema>` keeps its deserialization behavior and bounds.
This is a scoped correction to RFC-0001's typed Tool boundary; RFC-0001's other
acceptance gates and overall Draft status remain unchanged.

## User-facing design

The compiled [local Tool example](../../crates/stateknot-runtime/examples/local_tool_registration.rs)
registers the input through `register_rust_type::<IncidentLookup>` and the
output through `register_rust_output_type::<IncidentSummary>`, using separate
versioned schema URIs. It passes the returned references to `ToolDescriptor`,
constructs `ToolAdapter`, and freezes the executable in `ToolProviderRegistry`.
No extra runtime, registry, transport or dependency is introduced.

Both entry points use the existing offline registry and error types. Schema
generation does not authorize an invocation. A manually implemented `JsonSchema`
must still faithfully describe the corresponding Serde behavior; arbitrary Rust
serialization code cannot be proven equivalent by schema generation alone.

## Detailed semantics and failure behavior

1. Generate input and output in their respective explicit 2020-12 profiles.
2. The registry inserts the exact `$id`, validates the meta-schema, bounds the
   canonical document and total registry size, and verifies the digest.
3. `ToolAdapter::new` requires exact canonical equality for each descriptor pin.
   Missing resources, non-object input roots, stale digests and a different
   generated contract fail with `ToolAdapterBuildError::SchemaContract` and the
   input/output role before an executable binding is returned.
4. Runtime input is bounded and schema-validated before deserialization and
   application dispatch. Runtime output remains bounded, serialized and
   validated after the physical attempt. Existing truthful effect, provenance,
   cancellation and deadline rules are preserved.
5. `ToolProviderRegistry::resolve` continues to require the complete frozen
   descriptor, preventing a later same-identity descriptor substitution.

Input and output `JsonSchema` trait bounds remain enforced at compile time.
Compile-fail examples implement every required Tool method; successful controls
use the same public imports and methods with the missing derives restored.

## Persistence, compatibility and rollout

No database migration or durable encoding changes. No lockfile, dependency,
MSRV, protocol or credential behavior changes. The new helper is additive.
The existing input helper explicitly pins the dialect it already generates.

An output whose serialization and deserialization schemas have identical
canonical bytes retains its existing pin. If bytes differ, startup now rejects
the old input-shaped output contract. Operators must introduce new output
schema and Tool versions and retain the old executable revision for admitted
work requiring it. Do not overwrite a registered URI, reinterpret durable pins,
or silently rewrite admitted invocations. Changing the output profile is not a
database backfill. Rollback requires the previous source and its exact registry
snapshot; it cannot execute work admitted under incompatible new pins.

The alpha.1 registry release does not include this source increment. Publication
still follows the lockstep version, protected publishing and independent
consumer gates in `RELEASING.md`.

## Security, privacy and operations

Schemas remain trusted, local, immutable configuration. No schema URL retrieval
or user-defined schema execution is added. Directional generation reduces the
chance that application-only fields become output contract properties, but it
does not replace payload redaction, author review or tenant policy. Runtime
output validation remains necessary for custom schemas and conditional Serde.

Registration errors are startup diagnostics; no tool attempt or external effect
has occurred. Preserve error role and causal source through existing error
types. No new telemetry, credential payload or capacity assumption is required.
The existing finite schema and Tool resource limits remain mandatory.

## Alternatives considered

- Keep default generation and reject at runtime: discovers contract drift after
  an external effect and accepts an inaccurate startup binding.
- Change the old registration helper to serialization: changes every existing
  input caller and its canonical pins.
- Expose a general generation-settings API: expands dialect and contract
  configuration beyond the verified input/output use case.

The dedicated output helper preserves existing input callers and chooses the
actual output contract with one additional public method.

## Validation and acceptance

The public-boundary tests in
[`tool_registration.rs`](../../crates/stateknot-runtime/tests/tool_registration.rs)
use the production offline schema registry and executable provider registry:

- directional Serde rename, skipped local fields and omitted optional output
  register and produce the expected output through one actual Tool call;
- incorrect input is rejected with zero application calls;
- an output pinned through the old input profile is rejected at startup;
- missing, digest-substituted and role-substituted descriptor schemas fail
  before application dispatch;
- scalar input and invalid generated meta-schema cannot form a binding;
- a changed descriptor cannot resolve the frozen executable.

The core example uses explicit input/output profiles; the runtime example uses
the production output helper. Rust 1.88 compile-fail and positive-control
Rustdoc tests, format, Clippy, workspace tests, warnings-denied Rustdoc,
PostgreSQL 16/17, protocol, cross-platform and package gates must pass before
merging the implementation of this scoped contract.

The accepted scope is this additive source contract and correction. It does not
accept RFC-0001 as a whole, qualify a release or replace the final independent
security review. Implementation and release evidence remain separate.

## Unresolved questions

None for this correction. General schema equivalence, custom Serde correctness,
the remainder of R1 and final R6/R7 qualification remain separate gates.
