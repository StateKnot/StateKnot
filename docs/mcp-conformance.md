<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP conformance status

This page states exactly what the current executable evidence supports. It is
not a complete-client badge.

## Current claim

StateKnot implements two distinct MCP `2026-07-28` client-side Tool surfaces:

- [`McpRemoteTool`](mcp-remote-tool.md), a strict durable binding with reviewed
  server/schema pins and reconciliation-first ambiguous writes;
- [`McpClient`](mcp-client.md), a bounded general stateless Tool client with
  dynamic discovery, JSON/request-scoped SSE, `x-mcp-header`, and MRTR.
- [`McpOAuthAuthorization`](mcp-oauth.md), a challenge-driven interactive OAuth
  provider with discovery, PKCE, issuer migration, scope upgrade, refresh, and
  caller-owned durable stores.

The general client and OAuth provider pass all **32 scored client scenarios**
in the frozen official `2026-07-28` requirement set, including all 25 OAuth
scenarios. This is an evidence claim for the implemented pre-alpha client
profile, not an MCP Server, authorization-server, extension, stable-API, or
SDK-tier conformance claim.

## Frozen evaluation input

The evidence was produced on 2026-09-02 with:

- npm package `@modelcontextprotocol/conformance@0.2.0-alpha.11`;
- npm integrity
  `sha512-imPK9tx5gQsL6ZKQq4MrsyDYfSaIwpRmX6+ogjbeAXs9LGvxkBxWcY7KcS7TvwaBk/ZiVWl6b/naF4q83UwDRA==`;
- source `gitHead` `c321dd32035556e6769d3724a8ee97d87c3faaac`;
- protocol and frozen requirement revision `2026-07-28`;
- Rust `1.88.0` and Node.js `24.19.0`;
- no expected-failures file.

The package and full transitive dependency graph are exact in
`conformance/mcp-client/package-lock.json`. The observed platform manifest is
committed at
`conformance/mcp-client/evidence/2026-09-02-macos-arm64.json`.

The authoritative inventory is:

```console
npx --yes @modelcontextprotocol/conformance@0.2.0-alpha.11 list --requirements 2026-07-28
```

It contains 69 scored scenarios: 37 server and 32 client scenarios. The client
set contains seven non-OAuth scenarios and 25 OAuth scenarios.

## Result

| Official client inventory | Scenarios | Success | Skipped | Failure |
| --- | ---: | ---: | ---: | ---: |
| Required non-OAuth | 7 | 45 | 11 | 0 |
| Required OAuth | 25 | 328 | 0 | 0 |
| **Required total** | **32** | **373** | **11** | **0** |
| Explicitly not scored | 7 | 33 | 6 | 17 |

The three metadata skips are optional Roots, Sampling, and Elicitation
capability declarations that StateKnot does not advertise. The eight standard
header skips cover lifecycle, Resource, and Prompt methods outside this Tool
client surface. A skip is not counted as a pass and no unsupported capability
is claimed. The final row contains Client Credentials, Enterprise Managed
Authorization, DPoP, Workload Identity Federation, and a post-release JSON
Schema preservation scenario. The official requirement set reports these seven
scenarios but explicitly excludes them from scoring; their 17 failures do not
become expected failures and StateKnot does not claim those extensions.

The successful checks cover Tool invocation and wire-schema validity; required
request metadata and version retry; Tool-specific standard headers; primitive,
nested, null-omitting, and Base64 custom headers; individual rejection of
invalid annotated Tools; no network `$ref` dereference; and exact isolated MRTR
request-state behavior with fresh JSON-RPC IDs. OAuth checks cover all metadata
discovery variants, CIMD and pre-registration, scope source/omission/step-up,
three token endpoint authentication modes, resource mismatch, offline access,
authorization-server migration, and the complete RFC 9207 issuer matrix.

## Reproduce the gate

```console
cd conformance/mcp-client
npm ci --ignore-scripts
cd ../..
bash conformance/mcp-client/run-2026-07-28.sh
```

The script builds the real Rust driver and asks the pinned runner to execute the
entire frozen client requirement set: 32 required and seven explicitly
not-scored scenarios. It stores raw output below the ignored `results/`
directory, then independently requires the exact required inventory,
373-success/11-skip/zero-failure/zero-warning result, and the exact not-scored
inventory. Missing, duplicate, unexpected, or drifted required evidence fails
the command. No expected-failures file is used.

CI runs the same script with pinned Rust and Node toolchains. It does not use an
expected-failures baseline. The standalone HTTP/SSE contract additionally
tests fragmented request-scoped SSE, notification ordering, nested promoted
headers, credentials, and per-request metadata:

```console
cargo test -p stateknot-integrations --test mcp_client_contract --locked
```

## Why the strict durable profile remains separate

The official `tools_call` fixture intentionally advertises a Tool without an
output schema and returns text without `structuredContent`. That is valid for a
general client. `McpRemoteTool` must reject it because the durable binding
requires exact reviewed input/output schemas, a pinned server implementation,
locally validated structured output, durable-before-dispatch state, and
explicit reconciliation after an ambiguous write.

Passing the general-client fixture does not loosen that contract. The two
surfaces share bounded transport primitives but preserve different trust and
recovery guarantees.

## Remaining gates

The next independent gates are the MCP Server profile; broader Resources,
Prompts, and Task-extension surfaces; the seven currently not-scored client
extensions; stable SDK/API review; and release artifact publication. Each needs
its own implementation and applicable official evidence. A release claim must
publish generated checks and platform identity as release artifacts while
preserving the strict Remote Tool PostgreSQL recovery tests as independent
gates.

The source of truth for the runner is the
[official MCP Conformance repository](https://github.com/modelcontextprotocol/conformance).
Protocol behavior is defined by the
[MCP 2026-07-28 base protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic/index),
[Streamable HTTP transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http),
[Tool surface](https://modelcontextprotocol.io/specification/2026-07-28/server/tools),
and [MRTR pattern](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/mrtr).
