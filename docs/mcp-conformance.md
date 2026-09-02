<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP conformance status

This page states exactly what the current executable evidence supports. It is
not a complete-framework, stable-API, or extension badge.

## Current claim

StateKnot implements three distinct MCP `2026-07-28` boundaries:

- [`McpRemoteTool`](mcp-remote-tool.md), a strict durable binding with reviewed
  server/schema pins and reconciliation-first ambiguous writes;
- [`McpClient`](mcp-client.md), a bounded general stateless Tool client with
  dynamic discovery, JSON/request-scoped SSE, `x-mcp-header`, and MRTR.
- [`McpOAuthAuthorization`](mcp-oauth.md), a challenge-driven interactive OAuth
  provider with discovery, PKCE, issuer migration, scope upgrade, refresh, and
  caller-owned durable stores.
- the [MCP Server profile](mcp-server.md), a StateKnot-owned application for
  immutable Tools, Resources, Resource Templates, Prompts, optional Completion,
  and MRTR behind a strict stateless HTTP transport.

The general client and OAuth provider pass all **32 scored client scenarios**
in the frozen official `2026-07-28` requirement set, including all 25 OAuth
scenarios. The strict Server transport passes all **37 scored server
scenarios**. These are evidence claims for the implemented pre-alpha Client and
Server profiles, not an authorization-server, Tasks or other extension,
stable-API, SDK-tier, or complete-framework conformance claim.

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

## Client result

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

## Server result

| Official server inventory | Scenarios | Success | Skipped | Info | Failure | Warning |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Required total** | **37** | **114** | **5** | **1** | **0** | **0** |
| Pending, explicitly unscored gates | 3 | 32 | 0 | 0 | 0 | 0 |
| Reported Tasks extension | 10 | 12 | 1 | 0 | 30 | 0 |

The five skips are optional client-capability branches exercised by the
stateless fixture; they are not counted as passes. The single informational
check records the runner's multiple-SSE-stream observation and is not a
warning. The three pending gates cover JSON Schema 2020-12 and standard/custom
HTTP header validation. The frozen runner reports them but does not include
them in the 37-scenario score, so StateKnot preserves them as additional exact
regression gates without inflating the conformance claim.

The final row is the MCP Tasks extension. Its failures are intentional evidence
that Tasks are neither advertised nor implemented; they are not converted into
expected failures and no Task capability is claimed.

The scored Server checks cover stateless discovery and transport metadata,
JSON and request-scoped SSE, Tool listing/calls and mixed content, progress,
Resources and templates, Prompts and Completion, DNS-rebinding protection,
cache metadata, Resource-not-found behavior, and the complete core MRTR
request-state matrix.

## Reproduce the gate

```console
cd conformance/mcp-client
npm ci --ignore-scripts
cd ../..
bash conformance/mcp-client/run-2026-07-28.sh
bash conformance/mcp-server/run-2026-07-28.sh
```

The scripts build the real Rust drivers and ask the pinned runner to execute the
complete frozen Client and Server requirement sets. Raw output is stored below
ignored `results/` directories. Independent verifiers require the exact
inventories and status counts above, including the extra Server gates and the
reported-but-unclaimed extension rows. Missing, duplicate, unexpected, or
drifted evidence fails the command. No expected-failures file is used.

CI runs the same script with pinned Rust and Node toolchains. It does not use an
expected-failures baseline. The standalone HTTP/SSE contract additionally
tests fragmented request-scoped SSE, notification ordering, nested promoted
headers, credentials, and per-request metadata:

```console
cargo test -p stateknot-integrations --test mcp_client_contract --locked
cargo test -p stateknot-integrations mcp_server_ --locked
```

The official Server fixture deliberately mirrors the runner's application
names and payloads and uses the production `McpServerHttpService` transport. It
does not bypass the Host/Origin/body/version/authentication/admission/concurrency
boundary. The StateKnot-owned registry, authorization, schema, Resource,
Prompt, Completion, and result-limit layers are covered separately through
real HTTP service tests. This distinction prevents a fixture result from being
misrepresented as stable application-API certification.

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

The remaining independent gates are the Tasks extension, the seven currently
unscored Client extensions, stable SDK/API review, release artifact publication,
and full production qualification. Each needs its own implementation and
applicable official evidence. A release claim must publish generated checks and
platform identity as release artifacts while preserving both the StateKnot
application-layer HTTP tests and strict Remote Tool PostgreSQL recovery tests as
independent gates.

The source of truth for the runner is the
[official MCP Conformance repository](https://github.com/modelcontextprotocol/conformance).
Protocol behavior is defined by the
[MCP 2026-07-28 base protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic/index),
[Streamable HTTP transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http),
[Tool surface](https://modelcontextprotocol.io/specification/2026-07-28/server/tools),
and [MRTR pattern](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/mrtr).
