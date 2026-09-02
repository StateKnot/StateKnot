<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP OAuth Client Authorization

> 状态：已实现的 pre-alpha Surface；尚无稳定 API 或生产支持承诺。<br>
> Profile：面向 Stateless MCP `2026-07-28` Tool Client 的交互式 OAuth
> Authorization Code Flow。<br>
> 证据：冻结官方 `2026-07-28` Requirement Set 的 25 个计分 OAuth Client
> 场景全部通过，0 Failure。

`McpOAuthAuthorization` 是 `McpClient` 的 Challenge-driven Authorization
Provider。OAuth Metadata、Registration、PKCE State、Credential、Issuer
Migration、Scope Upgrade 与 Browser Handoff 不会进入 MCP Request Metadata，也不会
混入耐久 Agent State Model。

它不是通用 OAuth Library。一个 OAuth Manager 只绑定一个精确 MCP Resource；底层采用
固定版本的官方 MCP Rust SDK Authorization Engine，StateKnot 自己负责有界 Challenge
Capture 与 Replay Authority。

## 已实现 Profile

- 从 Bearer `WWW-Authenticate` Challenge 发现 Protected Resource Metadata，
  再发现 Authorization Server Metadata；
- 支持 Pre-registered Client、Client ID Metadata Document（CIMD）与 Dynamic
  Client Registration（DCR）Fallback，由 Host Policy 按优先级选择；
- Authorization Code + PKCE S256，并在 Authorization/Token Request 携带 RFC
  8707 `resource`；
- 根据 Server Metadata 选择 `client_secret_basic`、`client_secret_post` 或
  Public-client Token Endpoint Authentication；
- Challenge Scope、Metadata Scope、Scope Omission、有界 Step-up 与
  `offline_access`；
- 精确 Issuer Migration 与 RFC 9207 Authorization Response Issuer 校验；
- 通过调用方持有的 Credential Store 重用 Refresh Token；
- 每个 MCP Logical Request 默认最多执行一次 Authorization Challenge Replay，硬上限为 3；
- Code Exchange 前精确绑定 Callback Scheme、Host、Effective Port 与 Path；配置的
  Redirect URI 不允许 Query 或 Fragment；
- Resource、Metadata、Authorization、Redirect 与 Callback URL 的硬上限均为 16 KiB。

Client Credentials、Private-key JWT、Enterprise Managed Authorization、DPoP、
DPoP Nonce、Workload Identity Federation，以及发布后新增的 JSON Schema
Preservation 场景尚未实现，也不做支持声明。

## 使用耐久 Store 连接

Crate 尚未发布，请固定精确 StateKnot Revision。Host 必须持有 User-agent Integration
与 Durable Store：

```rust
use std::sync::Arc;

use stateknot_integrations::{
    McpClient, McpClientIdentity, McpClientOptions, McpOAuthAuthorization,
    McpOAuthCredentialStore, McpOAuthOptions, McpOAuthRegistration,
    McpOAuthResource, McpOAuthStateStore, McpOAuthUserAgent, ProviderEndpoint,
};

async fn connect<C, S>(
    user_agent: Arc<dyn McpOAuthUserAgent>,
    credential_store: C,
    state_store: S,
) -> Result<McpClient, Box<dyn std::error::Error>>
where
    C: McpOAuthCredentialStore + 'static,
    S: McpOAuthStateStore + 'static,
{
    let endpoint = ProviderEndpoint::https("https://mcp.example.com/mcp/")?;
    let resource = McpOAuthResource::from_endpoint(&endpoint)?;
    let registration = McpOAuthRegistration::client_metadata_document(
        "https://agent.example.com/oauth/client-metadata.json",
    )?;
    let oauth_options = McpOAuthOptions::native(
        "http://127.0.0.1:49152/callback",
        "Inventory Agent",
        registration,
    )?;
    let authorization = Arc::new(
        McpOAuthAuthorization::new_with_stores(
            resource,
            oauth_options,
            user_agent,
            credential_store,
            state_store,
        )
        .await?,
    );
    let client_options =
        McpClientOptions::default().with_maximum_authorization_retries(1)?;

    Ok(McpClient::connect(
        endpoint,
        McpClientIdentity::new("inventory-agent", "1.0.0")?,
        authorization,
        client_options,
    )
    .await?)
}
```

Client Credential 已由外部 Provision 时使用
`McpOAuthRegistration::pre_registered`。只有部署 Policy 明确接受 DCR
Compatibility 时才使用 `automatic()`。Confidential Secret 存放在 `ApiKey` 中，并始终
从 `Debug` Output 脱敏。

## User-agent 边界

可信 Host Application 实现 `McpOAuthUserAgent::authorize`。它接收已经过校验的
Authorization URL 与精确 Redirect URI，并返回完整 Callback URL。

Native Application 必须：

1. 打开 Browser 前先占用 Loopback Listener；
2. 只绑定配置的 Loopback Address、Port 与 Callback Path；
3. 打开提供的 URL，且不记录它；
4. 只接收一个有界 Request，拒绝其他 Path 或 Origin；
5. 返回完整 Callback URL，然后关闭 Listener。

StateKnot 施加独立的 5 分钟默认 Timeout（可配置但硬上限 24 小时），精确校验 Callback
Origin 与 Path，再由 OAuth Session 校验 State、Code、PKCE 与 RFC 9207 Issuer。取消或
畸形 Callback 作为 Permission Denial；Listener 不可用或 Timeout 作为 Unavailable。

## Durable Store 合约

`new()` 明确只在内存保存状态。需要跨重启恢复的生产进程必须使用
`new_with_stores`。Store Implementation 属于 Trusted Computing Base，必须提供：

- Access Token、Refresh Token、Client Secret 与 PKCE Verifier Material 的静态加密和密钥轮换；
- Tenant、Principal、MCP Resource 与 Authorization-server Issuer 隔离；
- 原子 Read/Write/Delete 与 Compare-before-replace Protection；
- Abandoned Authorization State 的 TTL Expiry 与一次性消费；
- 绝不包含 Token、Code、Verifier、Callback Query 或 Authorization URL 的脱敏 Telemetry/Audit Event。

不要把 In-memory Store 当作隐式生产耐久承诺。

## Challenge 与 Retry Authority

没有可用 Credential 时，第一个 MCP Request 匿名发送。有界 401/403 Bearer Challenge
会被复制、解析，并在 Outbound HTTP Attempt Timeout 之外处理。授权成功后，Client 使用新
JSON-RPC ID 将同一个 Logical MCP Method 重放一次。Timeout、Connection Failure 或
Ambiguous Tool Result 不会被重放。

403 只有同时显式包含 `error="insufficient_scope"` 与 Scope 才能触发 Scope Upgrade。
Scope-upgrade Attempt 与 Transport Replay 分别拥有独立有限上限。Authorization-server
Issuer 变化会禁止复用 Registration，并启动新的 Issuer-bound Registration Flow。

## 安全与上线清单

生产使用前：

1. MCP、Protected-resource Metadata、Authorization Metadata、Registration、
   Authorization 与 Token Endpoint 强制 HTTPS；HTTP 只用于显式受管 Loopback Flow；
2. 在 Client 外 Allowlist DNS/Egress，并在每次依赖升级时检查上游 MCP SDK 版本；
3. 使用 CIMD 时发布并固定精确 Client Metadata Document；
4. 使用系统 Browser 或 Hardened Managed User Agent，不使用收集 Credential 的 Embedded Web View；
5. Credential 只存入加密、Tenant-scoped Store；
6. 只用脱敏 Identifier 监控 Challenge、Discovery、Registration、Refresh、Migration 与 Scope Failure；
7. 除非已审核 Server Contract 证明需要其他有限值，否则保留 `maximum_authorization_retries(1)`；
8. Transport、OAuth SDK、URL Policy、Storage 或 Callback 发生变化后，重新运行固定官方 Requirement Gate。

复现证据：

```console
cargo test -p stateknot-integrations --test mcp_client_contract --locked
bash conformance/mcp-client/run-2026-07-28.sh
```

精确计分与不计分边界见 [MCP Conformance 状态](mcp-conformance.zh-CN.md)。
