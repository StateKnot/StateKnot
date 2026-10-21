<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0023: Case-insensitive media type input schemas

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-10-10
- Tracking: [R1 C2/C4 ledger](../r1-contract-gap-ledger.zh-CN.md), [PR 161](https://github.com/StateKnot/StateKnot/pull/161)
- Supersedes: None
- Superseded by: None

## Problem and decision

The immutable Core fuzz run on PR 161 found that `MediaType` accepts
`image/pngModelProviderReplayFormat` but its generated input schema rejects it.
The existing constructor, reader, documentation and compatibility fixtures
intentionally accept case-insensitive type/subtype names and normalize them to
lowercase. Its custom schema incorrectly uses the lowercase producer pattern
for both directions. This is an input-schema defect, not a new accepted input
representation or permission to interpret arbitrary bytes as a trusted image.

Select the existing lowercase pattern for serialization and an ASCII
case-insensitive pattern for deserialization/default generation. Keep the
constructor, strict reader, bounded MIME parser, parameter checks, serializer,
canonical bytes, wire digests, product dependencies and MSRV unchanged. The
corrected input description distinguishes accepted text from normalized output.
Nested artifact/content/Agent/Model types inherit the correction through the
existing generator; no outer entry point or fuzz oracle is bypassed.

## Compatibility and operations

Generated input/default schema bytes containing `MediaType` change. All
serialization pins remain exact. Review and enumerate every affected input pin
against the prior source; retain the existing wire fixtures and all unrelated
pins. Input/default schema changes must not be misrepresented as wire changes
or historical N-1/N-2 qualification.

The reviewed 35 changes and the actual prior `ArtifactRepresentation` input
document are retained in `core-media-type-input-schemas-v1.json`, generated from
source `c4967d0a57aacaae4e5ff8658c66810cba160203` and checked against its existing
pin. They cover `MediaType`, artifact/content/instruction/message containers,
Agent admission/descriptor/result, child admission, Model invocation/event/
request/response/transcript, Run lifecycle/transition, and Tool result/invocation
containers. The closed inventory continues to check all 279 unrelated input
pins and all 314 output pins. This is a prior-source regression baseline.

A typed Tool registry with a previous input pin must reject the changed
executable at startup before any application call. Use a new immutable schema
URI/version and Tool version for the corrected input schema. Retain the exact
previous executable and registry for already admitted work and rollback.
Never overwrite a registered schema, rewrite persisted pins or alter invocation
records to hide startup mismatches. There is no database migration, release,
protocol or credential change; published alpha.1 packages remain unchanged.

## Security and resource limits

Wildcard types, duplicate parameter names, unsupported parameter bytes and
the existing 512-byte/16-parameter/128-byte-value ceilings still fail in the
production reader. Schema validation remains an additional gate, not a MIME
sniffer, registry lookup or authorization decision. Case normalization cannot
grant access to artifacts or validate their content. Diagnostics and retained
reproducers contain only reviewed synthetic values.

## Validation and rollout

Retain the exact CI reproducer in `fuzz/seeds/core_readers/MediaType-mixed-case`
and assert that it reaches the actual reader, input validator, producer
validator and ordinary/canonical round trips. Cover uppercase, mixed case,
structured suffixes, parameter case, quoting and maximal name lengths. The
closed Core matrix must verify the reviewed changed input pins, every unchanged
output pin, old wire bytes and all unrelated input pins. Directional candidate
generation prints pins for review and never rewrites fixtures during tests.

All final-source workspace, PostgreSQL 16/17, protocol, cross-platform,
website, package, dependency and immutable bounded ASan fuzz gates must pass
before acceptance or merge. Keep the original failed run and reproducer as
evidence. This Draft correction does not close R1, RFC-0001, R6 or R7.

## Alternatives and open gates

Rejecting uppercase input would break the existing documented normalization
contract. Using the input pattern for output would change canonical producer
schema pins unnecessarily. Removing the input oracle would conceal the defect.
The directional correction preserves both intended contracts. Final-source
qualification and RFC acceptance remain open.
