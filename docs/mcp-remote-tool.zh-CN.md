<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 严格 MCP Remote Tool Profile

`McpRemoteTool` 是 StateKnot 首个已经实现的 MCP 边界。它把一个远程发现的 MCP Tool 适配为协议无关的 `ErasedTool`，同时禁止远端 Wire Metadata 改写本地 Risk、Schema、Retry 与 Durability Semantics。

这不代表完整 MCP 支持。当前实现是 MCP **Client-side Remote Tool Binding**：固定协议版本 `2026-07-28`，使用 Modern Discovery、Stateless Streamable HTTP 与 Complete JSON Response。

## 支持范围

- 精确协议版本 `2026-07-28`；
- 启动时执行 Modern `server/discover`，随后进行有界 `tools/list` 分页；
- 生产环境使用经过验证的 HTTPS；明确管理的本地 Sidecar 与测试可使用 Literal-loopback HTTP；
- 仅接受 Complete JSON Response；
- 每个 Binding 固定一个 Remote Tool Name 与一个预期 Server Name/Version；
- 远端发现的 Input/Output Schema 必须与权威本地 Registry 的 RFC 8785 Canonical Bytes 完全一致；
- 支持 Anonymous 或 Bearer Authorization，并提供 Attempt-scoped Secret Resolution 扩展 Trait；
- Request/Response Body、Discovery Page、Discovered Tool、Startup/Shutdown 时间与并发调用全部有界；
- Dispatch 前执行本地 Input Validation，响应后执行本地 Structured-output Validation；
- 将读写不确定性映射到 StateKnot 的耐久 Tool Failure Model。

Adapter 使用精确锁定的官方 MCP Rust SDK `3.2.0`，因此 StateKnot MSRV 为 Rust `1.88.0`。

## 冻结一个 Binding

本地 `ToolDescriptor` 与 Schema Registry 才是权威来源。Descriptor 必须来自审核过的配置，不能从不可信 MCP Annotation 自动生成。

```rust
use std::sync::Arc;
use stateknot_integrations::{
    McpHttpOptions, McpRemoteTool, McpServerIdentity, ProviderEndpoint,
    StaticMcpBearerAuthorization,
};

let adapter = McpRemoteTool::connect(
    local_descriptor,
    "lookup_issue",
    ProviderEndpoint::https("https://mcp.example.com/v1/")?,
    McpServerIdentity::new("issue-service", "2026.09.0")?,
    Arc::new(schema_registry),
    Arc::new(StaticMcpBearerAuthorization::new(api_key)),
    McpHttpOptions::default(),
)
.await?;
```

`connect` 只执行一次 Discovery：验证精确 Negotiated Version、要求 Tools Capability、检查自报 Implementation Name/Version、扫描有界 Catalog，并比较 Canonical Schema。Server 升级或 Schema 变化必须创建并注册新的 Binding。

TLS 按平台 Trust Store 验证配置 Endpoint。MCP Implementation Name/Version 是 Server 自报的 Discovery Metadata；固定它可以检测 Drift，但不构成密码学 Server Attestation。需要更强 Identity 的部署必须在 Adapter 外增加 mTLS 或已认证 Gateway。

## Authorization 与 Secret

`StaticMcpBearerAuthorization` 只适用于受控的单租户 Binding。多租户部署应实现 `McpAuthorizationProvider`，在 Startup 和每个已 Admission 的耐久 Attempt 中解析 Secret Handle。

Adapter 会：

- 取得 Per-binding Call Gate 后才解析 Attempt Credential；
- 不把 Credential 复制到 MCP Metadata；
- 从 `Debug` 输出中 Redact Authorization Object；
- 每次 Exchange 后重置内存 Authorization Slot；
- 禁用 Redirect 与 Transport Retry。

Secret Retrieval Failure 发生在 Dispatch 前，绝不会被错误转换为 Ambiguous Write Outcome。

## 调用与失败语义

每个已 Admission 的 Tool Attempt 只发送一次 `tools/call`。Response 必须 Complete、不得声明 `isError`、必须携带 `structuredContent`，且 Content Block 只能是 Text。Structured Output 经过有界处理与本地 Output Schema 校验后，才返回 `ToolResult`。

Remote Tool Annotation 只是不可依赖的 Hint，不能覆盖本地 Descriptor。特别是 `readOnlyHint`、`destructiveHint` 与远端 Schema 文本都不能静默改变 StateKnot Policy Decision。

对于本地声明的 Write，Dispatch 后发生任何 Transport/Protocol Loss，都映射为：

- `ToolExternalEffect::Unknown`；
- `FailureCategory::AmbiguousExternalOutcome`；
- `RetryAdvice::ReconcileFirst`。

Runtime 必须先通过 Status Query、Provider Intrinsic Key、Compensation 或 Human Decision 完成 Reconciliation，之后才能再次写入。Adapter 不会把不确定 Write 伪装成安全 Retry。

## 耐久 Reconciliation

`ToolReconciliationHandoff::result` 与 `ToolReconciliationHandoff::error` 会把权威证据绑定到精确的耐久 `Unknown` Invocation 与 Physical Attempt。构造过程复用 Core 中同一套 Invocation、Tool Identity、Attempt、Output Schema、Result Limit、Artifact Ownership、Risk/Effect 与 Retry Safety 不变量。

```rust,ignore
let handoff = ToolReconciliationHandoff::result(
    live_fence,
    unknown_invocation,
    EventId::generate(),
    authoritative_result,
)?;
let outcome = executor.commit_tool_reconciliation(handoff).await?;
```

提交 Result 前，Runtime 还会使用精确冻结的本地 Schema Registry 校验 Inline Output。随后 Executor 在一个 Fenced PostgreSQL Transaction 中追加独立 Reconciliation Audit Event 并推进 Tool Ledger，整个过程不会解析或调用 MCP Adapter。使用相同 Handoff/Event 重试会精确幂等。Lease Takeover 后，可用 `rebind_fence` 把保留证据绑定到同一 Run 的新 Live Fence。

带权威已知 External Effect 的 Error Evidence 会把 Ledger 解析为 `Failed`；Effect 仍为 `Unknown` 的证据会有意保留未决状态。该 API 是 Trusted Worker/Operations Boundary；HTTP 或 RPC Service 必须先授权 Evidence Submission，才能构造 Handoff。

## 当前明确拒绝

- Stateful MCP Session 与 `Mcp-Session-Id`；
- SSE Response 与 Legacy Initialization Fallback；
- Automatic Reconnect、Transparent Reinitialization 或 HTTP Retry；
- MRTR、Tasks、Incomplete Result、Progress Forwarding 与 Artifact/Resource Materialization；
- Image、Audio、Embedded-resource 与 Resource-link Result Block；
- 要求 StateKnot Idempotency Key 的 Descriptor，因为 Generic MCP 没有可安全注入该耐久 Key 的标准字段；
- 将 StateKnot 暴露为 MCP Server；
- Roots、Prompts、Resources、Sampling、Elicitation、Logging 或 MCP Apps。

这些排除项会在 Binding 或 Result Validation 阶段 Fail Closed，不会静默降级。

## 运维检查表

1. 审核并版本化本地 `ToolDescriptor`，包括 Risk、Resource Access、Idempotency、Status-query 与 Compensation Semantics。
2. 连接前在本地注册 Canonical Input/Output Schema。
3. 每个 Binding 只使用一个精确 HTTPS Endpoint 与 Expected Server Identity。
4. 生产 Credential 使用 Vault-backed `McpAuthorizationProvider`；不得写入 Descriptor 或 Log。
5. 把 `connect` 作为 Startup/Readiness 工作；Discovery Failure 会保持 Deployment Unready。
6. 将已连接 Adapter 注册进不可变 StateKnot Tool Registry。
7. 监控 Build Failure、Authorization Failure、Latency、Response Bound 与 Ambiguous Write，但不记录 Request Body 或 Credential。
8. Server/Schema 变化以新的已审核 Binding Rollout；不得原地修改 Live Binding。

## 可执行证据

Literal-loopback Contract Suite 证明：精确 Modern Discovery、Schema/Identity Pinning、Attempt Authorization Header、One-call 行为、不信任 Remote Annotation、Startup Schema-drift Rejection，以及 Lost Write Response 的 Ambiguous Mapping：

```console
cargo test -p stateknot-integrations --test mcp_contract --locked
cargo test -p stateknot-integrations --test mcp_durable --locked -- --test-threads=1
```

Durable Suite 会使用真实 PostgreSQL Store，并暂停一个真实 Loopback MCP Request，以便在 I/O 完成前现场观察 `Executing`；随后证明 Ambiguous Write Persistence、Duplicate Suppression、Authoritative Reconciliation 与单次网络调用下的 Idempotent Replay。CI 会在 PostgreSQL 16 和 17 上运行它。

这些 Suite 是 Profile Evidence，不是完整官方 MCP Client/Server Pass。固定版本的官方 Requirement Inventory，以及为什么不能把严格 Profile 误报为通用 Client，见[MCP Conformance 状态](mcp-conformance.zh-CN.md)。
