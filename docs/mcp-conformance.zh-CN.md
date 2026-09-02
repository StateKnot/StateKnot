<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Conformance 状态

本文只声明当前可执行证据能够支持的范围，不是完整 Client Badge。

## 当前声明

StateKnot 已实现两个相互独立的 MCP `2026-07-28` Client-side Tool Surface：

- [`McpRemoteTool`](mcp-remote-tool.zh-CN.md)：带已审核 Server/Schema Pin 与
  Reconciliation-first Ambiguous Write 的严格耐久 Binding；
- [`McpClient`](mcp-client.zh-CN.md)：支持动态 Discovery、JSON/Request-scoped
  SSE、`x-mcp-header` 与 MRTR 的有界通用 Stateless Tool Client。
- [`McpOAuthAuthorization`](mcp-oauth.zh-CN.md)：支持 Discovery、PKCE、Issuer
  Migration、Scope Upgrade、Refresh 与调用方 Durable Store 的 Challenge-driven
  交互式 OAuth Provider。

通用 Client 与 OAuth Provider 通过冻结官方 `2026-07-28` Requirement Set 中全部
**32 个计分 Client 场景**，其中包括全部 25 个 OAuth 场景。这是已实现 pre-alpha
Client Profile 的证据声明，不是 MCP Server、Authorization Server、Extension、
Stable API 或 SDK-tier Conformance 声明。

## 冻结的评估输入

证据于 2026-09-02 使用以下输入生成：

- npm Package：`@modelcontextprotocol/conformance@0.2.0-alpha.11`；
- npm Integrity：
  `sha512-imPK9tx5gQsL6ZKQq4MrsyDYfSaIwpRmX6+ogjbeAXs9LGvxkBxWcY7KcS7TvwaBk/ZiVWl6b/naF4q83UwDRA==`；
- Source `gitHead`：`c321dd32035556e6769d3724a8ee97d87c3faaac`；
- Protocol 与冻结 Requirement Revision：`2026-07-28`；
- Rust `1.88.0`、Node.js `24.19.0`；
- 不使用 Expected-failures File。

Package 与完整 Transitive Dependency Graph 精确固定在
`conformance/mcp-client/package-lock.json`。Observed Platform Manifest 位于
`conformance/mcp-client/evidence/2026-09-02-macos-arm64.json`。

权威清单命令为：

```console
npx --yes @modelcontextprotocol/conformance@0.2.0-alpha.11 list --requirements 2026-07-28
```

清单包含 69 个计分场景：37 个 Server 与 32 个 Client 场景。Client Set 包含 7
个非 OAuth 场景和 25 个 OAuth 场景。

## 结果

| 官方 Client 清单 | 场景 | Success | Skipped | Failure |
| --- | ---: | ---: | ---: | ---: |
| 必需非 OAuth | 7 | 45 | 11 | 0 |
| 必需 OAuth | 25 | 328 | 0 | 0 |
| **必需合计** | **32** | **373** | **11** | **0** |
| 官方明确不计分 | 7 | 33 | 6 | 17 |

3 个 Metadata Skip 是 StateKnot 没有声明的 Optional Roots、Sampling 与
Elicitation Capability。8 个 Standard Header Skip 属于本 Tool Client Surface
之外的 Lifecycle、Resource 与 Prompt Method。Skip 不计作 Pass，也不会据此声明不支持的能力。
最后一行包含 Client Credentials、Enterprise Managed Authorization、DPoP、Workload
Identity Federation 与发布后新增的 JSON Schema Preservation 场景。官方 Requirement
Set 会报告这 7 个场景，但明确不计分；其 17 个 Failure 不是 Expected Failure，
StateKnot 也不声明这些 Extension。

成功 Check 覆盖 Tool 调用与 Wire Schema；必需 Request Metadata 与 Version Retry；
Tool Standard Header；Primitive、Nested、Null-omitting、Base64 Custom Header；逐个
排除无效 Annotation Tool；禁止网络 `$ref` Dereference；以及使用全新 JSON-RPC ID、
精确隔离 Request State 的 MRTR。OAuth Check 覆盖全部 Metadata Discovery Variant、
CIMD 与 Pre-registration、Scope Source/Omission/Step-up、三种 Token Endpoint
Authentication Mode、Resource Mismatch、Offline Access、Authorization-server
Migration 与完整 RFC 9207 Issuer Matrix。

## 复现门禁

```console
cd conformance/mcp-client
npm ci --ignore-scripts
cd ../..
bash conformance/mcp-client/run-2026-07-28.sh
```

脚本构建真实 Rust Driver，并要求固定 Runner 执行完整冻结 Client Requirement Set：
32 个必需场景与 7 个官方明确不计分场景。官方原始 Output 保存到 Git 忽略的
`results/` 目录；独立 Verifier 强制校验精确必需清单、373 Success、11 Skip、0
Failure、0 Warning，以及精确不计分清单。必需证据缺失、重复、意外增加或 Drift
都会使命令失败，不使用 Expected-failures File。

CI 使用固定 Rust 与 Node Toolchain 执行同一脚本，不使用 Expected-failures
Baseline。独立 HTTP/SSE Contract 还会验证分片 Request-scoped SSE、Notification
顺序、Nested Promoted Header、Credential 与 Per-request Metadata：

```console
cargo test -p stateknot-integrations --test mcp_client_contract --locked
```

## 为什么严格耐久 Profile 仍然独立

官方 `tools_call` Fixture 会故意发布一个没有 Output Schema 的 Tool，并返回没有
`structuredContent` 的 Text；这对通用 Client 是合法的。`McpRemoteTool` 必须拒绝
它，因为耐久 Binding 强制要求精确已审核 Input/Output Schema、固定 Server
Implementation、本地校验 Structured Output、Durable-before-dispatch State，以及
Ambiguous Write 后的显式 Reconciliation。

通用 Client 通过 Fixture 不会放宽该合约。两个 Surface 共享有界 Transport
Primitive，但保留不同的 Trust 与 Recovery Guarantee。

## 剩余门禁

下一组独立门禁是 MCP Server Profile、更广 Resources/Prompts 与 Task Extension、
当前 7 个不计分 Client Extension、稳定 SDK/API Review，以及 Release Artifact
发布。每项都需要自己的实现与适用官方证据。未来发布声明还必须把生成的 Check 与
Platform Identity 作为 Release Artifact 发布，同时继续把严格 Remote Tool
PostgreSQL Recovery Test 保留为独立门禁。

Runner 的权威来源是[官方 MCP Conformance 仓库](https://github.com/modelcontextprotocol/conformance)。协议行为由
[MCP 2026-07-28 Base Protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic/index)、
[Streamable HTTP Transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)、
[Tool Surface](https://modelcontextprotocol.io/specification/2026-07-28/server/tools) 与
[MRTR Pattern](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/mrtr)定义。
