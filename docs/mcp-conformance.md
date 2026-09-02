<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP conformance status

This page records what StateKnot can and cannot claim about official MCP
conformance. It is an evidence report, not a compatibility badge.

## Frozen evaluation input

The inventory was refreshed on 2026-09-02 with:

```console
npx --yes @modelcontextprotocol/conformance@0.2.0-alpha.11 list --requirements 2026-07-28
```

The evaluated runner is:

- npm package: `@modelcontextprotocol/conformance@0.2.0-alpha.11`;
- npm integrity:
  `sha512-imPK9tx5gQsL6ZKQq4MrsyDYfSaIwpRmX6+ogjbeAXs9LGvxkBxWcY7KcS7TvwaBk/ZiVWl6b/naF4q83UwDRA==`;
- source `gitHead`: `c321dd32035556e6769d3724a8ee97d87c3faaac`;
- frozen requirement revision: `2026-07-28`.

The official requirement set contains 69 scored scenarios: 37 server scenarios
and 32 client scenarios. StateKnot does not currently implement an MCP server,
OAuth client, Roots, Prompts, Resources, MRTR, Tasks, or a general-purpose MCP
client. It therefore makes **no complete MCP client, server, or SDK tier
conformance claim**.

The authoritative runner and requirement-set behavior are documented by the
[official MCP Conformance repository](https://github.com/modelcontextprotocol/conformance),
and the protocol Tool requirements are defined by the
[MCP 2026-07-28 Tool specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/tools.mdx).

## Why the strict Tool profile is not scored as a pass

`McpRemoteTool` is a narrower deployment binding, not the general MCP client
role scored by the official requirement set. It deliberately requires all of
the following before registration or result commit:

- an exact reviewed local input and output schema;
- byte-identical input and output schemas discovered from the remote server;
- an exact expected server implementation name/version;
- complete `structuredContent` validated against the pinned output schema;
- one admitted `tools/call`, with transport retries disabled;
- explicit reconciliation after an ambiguous write.

The official required `tools_call` client fixture intentionally advertises a
tool without `outputSchema` and returns text content without
`structuredContent`. A conforming general-purpose client can call it, while the
StateKnot strict binding must reject it. Treating that rejection as a pass, or
loosening the production binding only for the runner, would misrepresent both
contracts.

StateKnot also does not carry an expected-failure file that turns this mismatch
green. The official runner explicitly treats a baselined failure as a failure
against a frozen requirement set; a baseline is regression control, not a
conformance grant.

## Executable evidence that does exist

The following tests are mandatory and exercise the implemented profile:

```console
cargo test -p stateknot-integrations --test mcp_contract --locked
cargo test -p stateknot-integrations --test mcp_durable --locked -- --test-threads=1
```

The first suite proves exact stateless discovery, protocol and standard request
headers, server/schema pins, attempt-scoped authorization, bounded one-call
behavior, schema-drift rejection, and reconcile-first lost write responses.

The second suite uses a real PostgreSQL store and a real loopback MCP exchange.
It pauses the server after receiving `tools/call` and proves that the invocation
is already durably `Executing`; then it loses the write response, proves
`Unknown`, suppresses duplicate dispatch, commits authoritative reconciliation,
and proves exact idempotent replay with one network call. CI runs this test on
PostgreSQL 16 and 17.

These are StateKnot profile tests, not substitutes for the official MCP
requirement set.

## Gate for a future conformance claim

A future general MCP client must be a separate surface from `McpRemoteTool`, so
broader interoperability cannot weaken the strict durable binding. Before any
client conformance claim, StateKnot must:

1. define and security-review the general client surface and its relationship
   to reviewed `ToolDescriptor` snapshots;
2. implement every scored client capability in the chosen frozen requirement
   revision, including OAuth and request-state behavior, or clearly avoid an
   SDK-tier claim;
3. run the exact frozen official requirement set in mandatory CI without
   unexpected failures or a misleading expected-failure blanket;
4. commit the generated checks, runner identity, command, platform, and date as
   a release artifact;
5. keep the strict remote Tool profile tests and PostgreSQL recovery proof as
   independent release gates.

Until those conditions hold, the accurate status is: **implemented strict MCP
2026-07-28 Remote Tool profile; official complete client/server conformance not
claimed**.
