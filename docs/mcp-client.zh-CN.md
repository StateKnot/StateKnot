<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 通用 Stateless MCP Tool Client

> 状态：已实现的 pre-alpha Surface；尚无稳定 API 或生产支持承诺。<br>
> Wire Profile：MCP `2026-07-28`、Stateless Streamable HTTP，仅 Tool Client。<br>
> 证据：强制本地 HTTP/SSE Contract，以及 [MCP Conformance 状态](mcp-conformance.zh-CN.md)记录的固定官方 Runner 门禁。

`McpClient` 是 `stateknot-integrations` 中面向互操作性的 MCP Client。它与
[`McpRemoteTool`](mcp-remote-tool.zh-CN.md) 明确分离；后者对已审核的
Identity/Schema Pin 与耐久 Reconciliation 有更强约束，动态 Catalog 无法提供这些保证。

发现并调用普通远端 Tool 时使用 `McpClient`。当外部写操作属于耐久 Agent
Run，并且必须保存 Admission、Ambiguity 与 Reconciliation 证据时，使用
`McpRemoteTool`。

## 已实现合约

- 一个不可变 HTTPS Endpoint；仅受管 Sidecar 与测试可使用 Literal-loopback HTTP；
- Stateless `server/discover`、有界 `tools/list` Pagination 与 `tools/call`；
- 每个请求必带 `_meta`、`MCP-Protocol-Version`、`Mcp-Method`，适用时带 `Mcp-Name`；
- JSON 与 Request-scoped SSE Response，包括最终匹配 JSON-RPC Response 前的有界 Notification；
- 对 String、Integer、Boolean 参数执行嵌套 `x-mcp-header` 投影；不安全 Header Byte 使用协议 Base64 Sentinel；
- 带无效 Header Annotation 的 Tool 会被单独排除，并保留可审计原因；
- Multi-round Tool Request（MRTR）：精确 Opaque `requestState`、全新 JSON-RPC ID、精确 Response Key 与并发调用隔离；
- Request-scoped Authorization、硬资源上限、禁用 Redirect、禁用通用 HTTP Retry；
- 有界捕获 401/403 Bearer Challenge，并且只有 Authorization Provider 显式批准后才使用新 JSON-RPC ID 恢复；
- JSON Schema 只作为不可信有界值保留，绝不通过网络解析 `$ref`。

独立的 [`McpOAuthAuthorization`](mcp-oauth.zh-CN.md) Provider 现已实现交互式
OAuth Authorization Code Profile。当前 Surface 仍不实现 Resource/Prompt Client、
Tasks、Roots、Sampling、Client Credentials、DPoP 或稳定 SDK Tier。独立的
[MCP Server Profile](mcp-server.zh-CN.md) 不会扩大该 Client 声明。Static Bearer
Credential 仍只是 Transport Credential，不代表 OAuth Flow。

## 连接、发现与调用

Crate 尚未发布。外部使用者目前必须固定精确 Git Revision，或直接在本 Workspace 中使用。

```rust
use std::sync::Arc;

use serde_json::json;
use stateknot_integrations::{
    ApiKey, McpClient, McpClientIdentity, McpClientOptions, McpToolCall,
    ProviderEndpoint, StaticMcpBearerAuthorization,
};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = ProviderEndpoint::https("https://mcp.example.com/mcp/")?;
let token = std::env::var("MCP_TOKEN")?;
let client = McpClient::connect(
    endpoint,
    McpClientIdentity::new("inventory-agent", "1.0.0")?,
    Arc::new(StaticMcpBearerAuthorization::new(ApiKey::new(token)?)),
    McpClientOptions::default(),
)
.await?;

let catalog = client.list_tools().await?;
for rejected in catalog.rejected_tools() {
    eprintln!("excluded MCP Tool {:?}: {}", rejected.name(), rejected.reason());
}

let lookup = catalog.find("inventory_lookup").ok_or("Tool is unavailable")?;
let response = client
    .call_tool(lookup, json!({ "sku": "STK-001" }))
    .await?;

for notification in response.notifications() {
    println!("notification method: {}", notification.method());
}

match response.into_outcome() {
    McpToolCall::Complete(result) => {
        if result.is_error() {
            eprintln!("the remote Tool completed with a Tool-level error");
        }
        println!("content blocks: {}", result.content().len());
    }
    McpToolCall::InputRequired(_) => {
        eprintln!("the Tool requires an application-mediated follow-up");
    }
    _ => return Err("unsupported future MCP Tool outcome".into()),
}
# Ok(())
# }
```

Tool Descriptor、Description、Annotation、Content、Structured Content、
Notification、Server Instruction 与 Schema 均不可信。Host 必须先执行自己的
Policy 与 Schema Validation，才能把它们暴露给 Model、用户、Filesystem、Network
或 Durable State。

## Multi-round Tool Request

`input_required` Result 会转换为 `McpInputRequired`。Pending Value 拥有原始
Client、Tool、Arguments 与精确 Opaque Request State。`resume` 会消费它，Safe API
无法重复使用同一份 State。

Host 必须检查 `input_requests()` 中的每个 Entry，并且只把受支持的方法交给正确的可信子系统；之后必须为每个请求 Key 精确返回一个 Entry，多余或缺失 Key 会在本地被拒绝。StateKnot 不会自动批准 Elicitation、虚构 Roots 或静默调用 Sampling。

```rust
use serde_json::{Map, Value};
use stateknot_integrations::McpToolCall;

# async fn resume(
#     pending: stateknot_integrations::McpInputRequired,
#     reviewed: Map<String, Value>,
# ) -> Result<(), stateknot_integrations::StatelessMcpClientError> {
let response = pending.resume(reviewed).await?;
match response.into_outcome() {
    McpToolCall::Complete(_) | McpToolCall::InputRequired(_) => {}
    _ => {}
}
# Ok(())
# }
```

每轮请求使用全新 JSON-RPC ID。Server 提供 `requestState` 时，StateKnot 精确回传原始 String Byte，且不把它暴露给应用重建；Server 未提供时，Retry 会省略该字段。

## Transport、Authorization 与 Retry Authority

生产 Endpoint 强制 HTTPS。HTTP 只能通过 `ProviderEndpoint::loopback_http` 使用；
该构造器只接受 Literal Loopback IP，并拒绝 `localhost`、URL Credential、Query 与
Fragment。Redirect 与 Reqwest Retry 均被禁用。

每个 POST 都独立调用 `McpClientAuthorizationProvider::resolve`，因此生产
Credential Provider 可以轮换短期 Token，而不把 Secret 放入 `_meta`、Log 或 Debug
Output。`AnonymousMcpAuthorization` 与 `StaticMcpBearerAuthorization` 分别覆盖匿名与固定 Token 部署。`McpOAuthAuthorization` 进一步处理有界 Bearer Challenge、
Protected-resource/Authorization-server Discovery、Pre-registration/CIMD/DCR、
PKCE、Token Refresh、Issuer Migration、Scope Step-up 与精确 Callback Validation。
生产级跨重启恢复要求调用方提供加密 Credential Store 与带 Expiry 的 PKCE State Store。

Timeout、Connection Failure 或不确定 Tool Dispatch 后不会自动 Retry。Server 明确
声明支持 `2026-07-28` 时，Protocol Version Negotiation 获得一次 Retry。Authorization
Challenge 只有 Provider 显式批准后才获得 Replay，默认一次、硬上限三次，并使用新
JSON-RPC ID。需要恢复保证的外部写操作必须进入 `McpRemoteTool` 与耐久 Invocation Executor。

## 默认资源上限

默认值均有限，可通过 `ProviderHttpOptions` 与 `McpClientOptions` 进一步降低：

| 资源 | 默认值 |
| --- | ---: |
| 单个 Logical Request Deadline | 30 秒 |
| 每个 Client 并发请求 | 16 |
| Catalog Page / Advertised Entry | 16 / 1,024 |
| Request / Complete JSON Response | 16 MiB / 2 MiB |
| SSE Line / Event / Total Stream | 512 KiB / 2 MiB / 64 MiB |
| 每个 Response 的 Notification | 1,024 |
| Authorization Replay / Challenge Byte | 1 / 64 KiB |

Hard Implementation Ceiling 防止配置静默变为无界；Logical Request Deadline
不能配置为超过 24 小时。Cursor Cycle、重复可用 Tool Name、来自另一 Client 的 Tool Descriptor、不安全 Integer Header、超限 Payload、
不匹配 Response ID、SSE 中独立 Server Request 与畸形 Result Type 都会通过
`StatelessMcpClientError` Fail Closed。

## 验证与上线清单

```console
cargo test -p stateknot-integrations --test mcp_client_contract --locked
bash conformance/mcp-client/run-2026-07-28.sh
```

部署前：

1. 固定 StateKnot Revision 与 MCP Wire Profile；
2. Allowlist HTTPS Endpoint，并在 Client 外控制 DNS/Egress；
3. Token 会过期时使用可轮换的 Request-scoped Provider，或使用带加密 Tenant-scoped Store 的 OAuth Provider；
4. 把 Discovery 与 Tool Output 当作不可信输入；
5. 把限制设为低于下游 Model、Proxy 与 Storage 的上限；
6. 决定是否 Retry 前先分类 Tool Side Effect；
7. 需要 Reconciliation 的写操作使用严格耐久 Binding；
8. 监控 Rejected Tool、Protocol Failure、Timeout 与 Tool-level `isError`，但不记录 Credential 或敏感 Payload。

固定官方证据覆盖 `2026-07-28` Requirement Set 中全部 32 个计分 Client 场景，
包括全部 25 个计分 OAuth 场景：373 项计分 Assertion Success、0 Failure；11 项
Optional 或未实现方法检查被跳过。7 个官方明确不计分的 Extension 单独报告，且不进入
StateKnot 的 Client、Server、Extension 或 SDK-tier 声明。
