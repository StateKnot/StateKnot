<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Server Profile

> 状态：已实现的 pre-alpha Profile；Public API 尚不稳定。<br>
> 协议版本：仅 `2026-07-28`。<br>
> Transport：Stateless Streamable HTTP Complete JSON 与 Request-scoped SSE。<br>
> 明确排除：MCP Tasks Extension 尚未实现，也不做支持声明。

StateKnot 现已提供由自身类型定义的 MCP Server Application Layer，覆盖 Tools、
Resources、Resource Templates、Prompts、Completion，以及 Tool/Resource/Prompt 的
多轮请求（MRTR）。官方 Rust SDK 只作为私有 Wire Adapter，其领域类型不会成为
StateKnot Public API 的组成部分。

该 Server 仍属于 pre-alpha。此实现不等于整个框架已经 Production-ready，也不代表
Rust API 已稳定、已经内置 OAuth Authorization Server，或已经支持 Tasks Extension。

## 生产边界

一个请求按以下顺序穿过边界：

```text
Host/Origin/Body 检查
  -> Bearer Authentication
  -> 跨副本 Admission Policy
  -> 解码 Method 与 Resource Lookup
  -> Scope 与 Operation Authorization
  -> Schema/Argument Validation
  -> Application Handler
  -> 有界且通过 Schema 校验的 Result
```

Transport 不会把 `Mcp-Method` 或 `Mcp-Name` 当作 Authorization Fact。在 JSON-RPC
Request 完成解码与校验前，它们只是 Hint。只要配置中包含非字面 Loopback Host，
匿名服务就会在启动时被拒绝。

`McpServerHttpService` 提供：

- 精确 `2026-07-28` Version Enforcement 与 Stateless Protocol Metadata；
- 不允许 Wildcard 逃逸的显式 Host/Origin Allowlist；
- Streaming Request Body、Concurrency 与 Request Deadline 上限；
- Credential Redaction 与固定 RFC 6750/9728 Challenge 的 Bearer Authentication；
- 由调用方实现的 Admission Interface，用于共享 Quota 与 Rate Limit；
- Cooperative Shutdown 与 Cancellation Propagation；
- Complete JSON 与 Request-scoped SSE；禁用 Legacy Session 和 `initialize`。

生产部署必须在公网边界启用 TLS、Bearer Authentication 与跨副本 Admission 实现。
`anonymous_loopback()` 仅用于本地开发与 Hermetic Conformance Run。

## Application Surface

`McpServerApplicationBuilder` 只组合实际配置的 Surface，并且只声明对应 Capability。
调用缺失 Surface 会返回 Method-not-found；StateKnot 不会用空实现伪造 Capability。

### Tools

`McpServerToolRegistryBuilder` 在启动时冻结 Executable Tool Registry。它会校验
Portable Name、JSON Schema 2020-12、Catalog/Schema Byte Ceiling、重复项、稳定排序与
Canonical Registry Digest。Validator 只在 Offline 模式编译；未解析或网络 `$ref`
会使启动失败。

每个 Call 先完成资源上限检查，再执行 Policy。Authorization 早于 Input Schema
Diagnostic，因此被拒绝的 Principal 无法探测私有 Tool Schema。Handler 仅在授权与
输入校验成功后运行。Structured Result 离开进程前必须符合注册的 Output Schema。
Text、Image、Audio、Embedded Resource、Resource Link 与未来协议 Content 都有明确
边界并经过校验。

只有请求携带 Progress Token 时才能上报 Progress；Handler Cancellation 采用协作式
传播。Tool-level Failure 保持 Tool Result，Transport、Policy 与 Handler Failure 保持
Protocol Error。

### Resources 与 Templates

不可变 Resource Catalog 校验 Absolute URI、结构化 URI Template、Catalog Ceiling，
并冻结 Stable Digest。Read Authorization 先于 Resource Existence Disclosure。Text 与
Binary Content 会校验 MIME、Base64、Item Count 和 Aggregate Bytes。每个 Result 都
携带显式 TTL 与 Public/Private Cache Scope。

### Prompts 与 Completion

Prompt Catalog 校验 Name、唯一且有界的 Argument、Required Field、Scope、排序与
Stable Digest。Authorization 早于存在性和参数诊断。渲染结果使用有界的 StateKnot
Text、Image、Audio 与 Embedded-resource Content。

Completion 是可选能力，因此只有配置 Provider 后才会声明。Provider 收到有界且已
认证的 Prompt/Resource Template Reference、当前 Argument 与仅 String 的 Context。
Result 最多包含 100 个唯一值，并强制 Pagination Metadata 自洽。Target-specific
Completion Authorization 由 Provider 负责。

## Scope-aware Discovery 与 Cache

Tool、Resource、Resource Template 与 Prompt Discovery 会按 Principal 的精确 Scope
过滤。Private Cursor 绑定 Catalog Digest、Principal Subject、Canonical Scope Set、
Surface 与 Offset；不能跨身份或跨 Catalog Revision 重放。只要 Catalog 中存在
Scope-restricted Entry，Public Cache 配置就会在启动时被拒绝。

Annotation、Description、Schema、Server Instruction、Client Capability、Completion
Value 与 Transport Header 都只是 Data，永远不是 Authority。

## 多轮请求与 Request State

Tools、Resources 与 Prompts 可通过 `input_required` 请求 Elicitation、Sampling 或
Roots Input。StateKnot 校验 Request Count、ID、Payload Size、Client Response 与
Opaque Request-state Size。

`McpServerRequestStateCodec` 使用显式 Keyring、Expiry 与 Associated Data 封装
Application JSON。调用方通过 Request Context 将 State 绑定到已认证 Principal 和
精确 Operation。Key 至少 32 Bytes、支持轮换、不会出现在 `Debug`，TTL 最长 24 小时。
Invalid、Expired、Tampered 或 Cross-operation State 会收敛到同一个 Public-safe Error。

不要在 Request State 中保存 Secret。Sealing 提供 Integrity 与 Binding；Payload 仍需
遵守 Application 自身的 Retention 与 Privacy Policy。

## 构建轮廓

Crate 尚未发布，因此完整可执行 Example 目前保留在 Crate Test 中。生产构建顺序为：

```rust,ignore
let options = McpServerApplicationOptions::new(
    "inventory-mcp",
    "1.0.0",
    100,
    Duration::from_secs(60),
    McpServerCacheScope::Private,
)?;

let app = McpServerApplicationBuilder::new(options)
    .with_tools(tool_registry, tool_authorization)?
    .with_resources(resource_catalog, resource_reader, resource_authorization)?
    .with_prompts(prompt_catalog, prompt_renderer, prompt_authorization)?
    .with_completion_provider(completion_provider)?
    .build()?;

let service = McpServerHttpService::with_admission_control(
    app,
    http_options,
    McpServerAuthentication::bearer(authenticator, bearer_challenge),
    admission_control,
)?;
```

把 `service` 挂载到 Axum、Hyper 或其他兼容 Tower Host 的一个精确 Endpoint。不要在
每个请求中重建 Registry；必须在接收流量前构建并校验全部 Definition。

## 验证证据

冻结的官方 Runner 为 `@modelcontextprotocol/conformance@0.2.0-alpha.11`，Source
Revision 为 `c321dd32035556e6769d3724a8ee97d87c3faaac`，Requirement Revision 为
`2026-07-28`。

严格 Transport Fixture 精确通过全部 37 个计分 Server Scenario：114 项 Assertion
Success、5 项显式 Capability Skip、1 项 SSE Info、0 Failure、0 Warning。另有 3 个
Pending 且不计分的 JSON Schema/HTTP Header Gate，共 32 项 Assertion Success、0
Failure、0 Warning。StateKnot Application Surface 另通过真实 HTTP Boundary Test，
覆盖 Capability Discovery、Pagination、Authorization Ordering、Schema Validation、
Tool Dispatch、Resource Read、Prompt Rendering、Completion、MRTR Binding 与 Result
Limit。

```console
cargo test -p stateknot-integrations mcp_server_ --locked
bash conformance/mcp-server/run-2026-07-28.sh
```

官方 Fixture 为匹配 Conformance Inventory 而设计，并使用生产
`McpServerHttpService` Transport。它是 Acceptance Evidence，不是 Application
Template。StateKnot 自有 Registry 与 Policy Layer 由上述独立 HTTP Test 覆盖。完整
Inventory 与 Claim Rule 见 [MCP Conformance 状态](mcp-conformance.zh-CN.md)。

## 不做声明的能力

- MCP Tasks、Task Lifecycle、Task Notification 或 Task/MRTR Composition；
- Deprecated Stateful Session 或 Legacy `initialize` Flow；
- 内置 OAuth Authorization Server 或 Identity Provider；
- Dynamic Catalog Mutation 或 List-changed Notification；
- MCP Apps 或其他 Extension；
- Stable Rust API、crates.io Release 或 SDK-tier Certification；
- 整个 StateKnot Framework 的 Production Qualification。
