<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0020: Core optional producer schema profiles

- Status: Accepted
- Authors: StateKnot contributors
- Created: 2026-10-09
- Accepted: 2026-10-09 (source-only optional producer profiles; all merge gates below apply)
- Tracking: [R1 acceptance ledger, C2/C4](../r1-contract-gap-ledger.zh-CN.md), [Issue 149](https://github.com/StateKnot/StateKnot/issues/149)
- Supersedes: None
- Superseded by: None

## Problem and decision

RFC-0019 registers typed Tool outputs using the serialization profile. Three
custom Core `JsonSchema` implementations delegate to private deserialization
wires whose optional fields have `serde(default)` but do not declare the
serializer's omission rule. `Failure` output therefore requires `details` and
`caused_by_event_id`, although the actual serializer omits absent values.
`ToolError` has the same issue for `recovery_handle`; deprecated/retired
`CapabilityLifecycle` variants have it for `sunset_at` and `replacement`.
Nested failures inherit the incorrect generated output contract.

Generate serialization schemas from the existing private borrowed producer
wires, which already declare `skip_serializing_if = "Option::is_none"` on these
six fields. Generate deserialization schemas from the unchanged reader wires.
The custom `JsonSchema` implementation selects by the generator contract. No
new public type, trait, method or duplicate serializer is added.
The deserialization profile and actual serialized bytes retain their existing
contracts. Unknown fields, duplicate fields, error phase/effect rules and
unknown-outcome reconciliation continue through their existing readers.

## Compatibility and rollout

The 42 previous fixture documents remain byte-identical. All 308 existing
deserialization/default schema pins remain exact. A separate closed inventory
pins all 308 current serialization schemas, including `BudgetRemaining`;
`GraphNodeSource` still has no `JsonSchema` contract.

An output containing one of the corrected types can have different generated
schema bytes. A frozen registry with an old output pin must fail exact startup
comparison. Operators introduce a new schema URI/version and Tool version,
retain the old executable revision and registry for already admitted work, and
never overwrite an immutable URI, rewrite a durable pin or backfill invocation
records. Rollback requires the matching previous executable and registry.

There is no database migration, protocol, credential, product dependency, MSRV
or published-version change. The correction is source-only until a subsequent
lockstep preview release passes the protected publishing and consumer gates.

## Validation and safety

The closed Core matrix compares both schema profiles for every reviewed type.
The isolated qualification workspace uses the same 307-reader list and checks
actual producer output against offline serialization validators. Regression
cases cover absent and present failure details/event references, Tool recovery
handles, and deprecated/retired lifecycle options. Public Tool tests use the
actual immutable schema and executable registries to verify dispatch and stale
output-pin rejection before an application call.

Three ASan/libFuzzer targets cover bounded JSON/JCS, typed readers, and the real
offline schema registry. Fixed engine/compiler versions, frozen dependency
versions, explicit source identity, input/depth/node/count/byte ceilings,
per-input deadlines, process ownership and retained reproducers make the
qualification bounded and repeatable. Inputs stay synthetic. Fuzzing does not
disable integrity, authorization or other production checks via `cfg(fuzzing)`.
The dev-only libFuzzer dependency has an exact-version NCSA permission in the
isolated policy; product dependency policy and advisory exclusions are unchanged.

All workspace, PostgreSQL 16/17, cross-platform, protocol, website, package,
dependency and bounded fuzz gates must pass on the final source before merge.
This decision does not accept all of RFC-0001, establish exhaustive fuzz/variant
coverage, qualify historical N-1/N-2 data, or replace R6/R7 and independent review.

## Alternatives

- Relax runtime output validation: would accept incorrect executable contracts
  and discover errors after external effects.
- Switch all generation to deserialization: would undo RFC-0019's directional
  guarantees and keep conditional output shapes inaccurate.
- Add new public output structs: would expand the API without changing the
  existing serializers, whose omission behavior is already correct.

Reusing the actual private producer wires corrects the source of the mismatch.

## Unresolved questions

None for these six fields. General custom Serde equivalence, remaining variant
combinations, historical migration and overall R1/R6/R7 remain separate gates.
