<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0021: Core object reader shapes

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-10-09
- Tracking: [R1 acceptance ledger, C2/C3/C4](../r1-contract-gap-ledger.zh-CN.md), [Issue 149](https://github.com/StateKnot/StateKnot/issues/149)
- Supersedes: None
- Superseded by: None

## Problem and decision

Core publishes canonical JSON wires and immutable generated input schemas.
Serde's struct and internally tagged enum readers can also consume positional
sequences, although those wires are declared as JSON objects. For example, a
schema reference's three array elements can enter the same fields as its object
form, and nonempty tagged transitions can consume a tag followed by positional
fields. `deny_unknown_fields` does not close this alternate shape.

A source audit of all 307 reviewed reader types reproduced 156 distinct
object-producing readers accepting a positional representation; 130 of their
root schemas directly declare `type: object`, with the remaining tagged schemas
expressing object alternatives. Those are selected current vectors, not all
variants or a claim about every deserializer shape. The implementation must
review every object reader, including types whose selected vector already
rejects its corresponding array.

Require a map at every Core JSON reader whose declared representation is an
object, including all alternatives of internally tagged object enums. Preserve
the existing private owned wire, field validation, constructor checks and
integrity verification. Directly derived public object readers receive the same
explicit private wire treatment. Scalar and collection representations retain
their specified shapes; BoundedJson remains a JSON value and Extensions retains
its intentional open-key map semantics. An ordinary object inside a collection
still requires its own object reader.

Use a private streaming visitor that calls `deserialize_map`, delegates its
`MapAccess` through Serde's `MapAccessDeserializer` to the existing owned wire,
and then runs the unchanged constructor/integrity path. Do not parse a Value
first, clone or reparse an unbounded document, suppress duplicate fields, add a
permissive schema union, or hide the mismatch at selected outer entry points.
The map guard has no authority and does not replace the existing bounded JSON,
size/depth/count, schema, tenant or authorization gates.

## Compatibility and rollout

Valid object fields, tags, optional omission/default rules, canonical bytes,
wire digests and all 308 input/default and 308 serialization pins remain exact.
No new public type, method, dependency or MSRV change is required. Keep every
existing compatibility fixture byte-identical. Adding new reviewed negative
vectors must not rewrite old documents or claim to be N-1/N-2 history.

Previously accepted positional JSON is outside the published object contract
and now returns a deserialization error. Consumers must send the object shape
from their pinned schema; there is no positional-data backfill, durable record
rewriting or new schema URI for unchanged canonical wires. Unexpected existing
store bytes continue through corruption/quarantine behavior and cannot dispatch
an external call. No credential, protocol-version or database migration is
introduced. Restore/rollback remains tied to each deployment's exact executable
and immutable registry rather than being inferred from the unchanged pins.

These domain objects promise their documented canonical JSON form, not
sequence-based binary-format compatibility. A Serde format that supplies an
object only as a sequence is rejected by the new object reader. This limitation
must be explicit in the public Core guide. Formats that expose a map may use the
same field reader, but receive no new cross-format support promise. Collections
and scalar Serde contracts remain unchanged.

The correction is source-only until a subsequent lockstep preview publication
passes its protected release and independent consumer gates. It does not change
the contents of the already published alpha.1 package.

## Validation and safety

1. Keep the compiler-checked 570-export, 307-reader and dual 308-pin inventories.
   Compare every valid selected fixture, wire/canonical digest and schema pin.
2. For each object-producing reader, generate an ordered positional candidate
   from its real serializer fields, preserving actual declaration order rather
   than the lexical key order of a Value. Reject it via both JSON text and Value
   readers, including empty, truncated and extended variants and tag-bearing
   forms. Retain additional nested and tagged-payload controls; full C2 variant/option
   enumeration remains a separate gate.
3. Replace nested object positions with their valid positional equivalents in
   representative schema, identity, authorization, content, lifecycle,
   admission, budget, journal, checkpoint, invocation and child-Join records.
   The outer reader must reject rather than silently normalize them.
4. Preserve raw duplicate/unknown/required-field rejection and ordinary object
   key-order independence. Keep strict tagged-unit regression controls.
5. Prove the real typed Tool registry rejects invalid input before its
   application call, while object inputs, outputs and exact immutable pins
   continue to dispatch. Preserve store round-trip, integrity and quarantine
   regressions on PostgreSQL 16 and 17.
6. Retain reviewed synthetic reproducer seeds. The immutable ASan/libFuzzer
   qualification must keep all production constructors, integrity checks and
   output validators enabled; newly found failures require actual boundary
   fixes, never changed pins or removed oracles.
7. Full final-source workspace, cross-platform, PostgreSQL, protocol, website,
   packages, dependencies and bounded fuzz gates must pass. Bilingual adopted
   usage and compatibility notes must match the merged source.

Schema mismatch alone does not demonstrate an authorization bypass. Review
real affected input boundaries separately; suspected vulnerabilities follow
SECURITY.md's private reporting process. This decision preserves fail-closed
validation and reduces alternate representations, but cannot replace R7's
independent security qualification, C6 history, R6 capacity or overall R1/RFC-0001
acceptance. Those gates remain open.

## Alternatives

- Permit arrays in generated input schemas: would establish an unnecessary new
  positional wire contract, including field-order and omission compatibility.
- Validate only at the typed Tool adapter: leaves direct and nested Serde/store
  readers with a different public wire contract.
- Materialize every object as a JSON Value: adds memory and can erase raw field
  duplicates before the existing reader checks them.
- Reimplement all field parsers in a custom JSON parser: duplicates Serde and
  existing constructor checks without changing the required shape guarantee.

A streaming map guard reuses the actual owned field reader and preserves its
existing validation. Public object readers that currently rely on derive need
an explicit owned wire, following the pattern already used for checked Core
objects.

## Unresolved questions

Before acceptance, review the map requirement for all 181 object readers and
verify their unchanged public items, private Serde field/variant attributes and
constructor/integrity paths. The complete C2 combination/history audit remains
a separate gate. Generic binary sequence support is explicitly excluded; no new
format or production readiness is implied by this source correction.
