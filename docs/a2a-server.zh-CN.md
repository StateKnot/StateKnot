<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# A2A 1.0 Server Profile

> 状态：已实现的 pre-alpha Server Profile；公开 Rust API 尚未稳定。<br>
> 协议：A2A `1.0`。<br>
> Binding：HTTP+JSON 与 JSON-RPC 2.0，包含 SSE Streaming。<br>
> 本页范围排除：Client 行为/证据与 gRPC Binding。Client 已独立实现并另行记录；
> gRPC 尚未实现。

StateKnot 可以暴露 A2A Server，但不会把官方 SDK 变成领域模型。
`A2aAgentCard`、Message、Part、Task、Artifact、Status Update、Push Config
与 Request 都是 StateKnot 自有的有界合约；固定版本的官方 `a2a-rs` SDK
只存在于私有 Wire Adapter 后。

这是可用于生产集成的 HTTP 边界，但不是完整的生产 Task Backend。
应用必须通过显式 Trait 提供 Authentication、Authorization、Replica-wide
Admission、耐久 Task Projection、Stream、Cancellation 与可靠 Push Delivery。

## Request Boundary

每个非 Discovery Request 都按以下顺序执行：

```text
shutdown / Host / Origin / canonical path / Content-Type / body ceiling
  -> 进程内 Request Admission
  -> Bearer Authentication（先于 Body Parsing）
  -> A2A Wire Decode 与有界 Contract Validation
  -> 基于 Decoded Operation 授权（先于 Task/Config Lookup）
  -> 调用方实现的 Replica-wide Quota Admission
  -> 耐久 A2aTaskService Operation
  -> 有界 Response 或有界 SSE Stream
```

公开 Agent Card 位于 `/.well-known/agent-card.json`，支持 `Cache-Control`、
`ETag`、`Last-Modified` 与 Conditional Request。它是唯一不执行 Credential
Authentication 的路由。启动时，Card 声明的 Capability 和 Interface URL
必须与 Task Service 及挂载路径完全一致，否则构建失败。

边界不接受 Forwarded-host Shortcut 或通配 Authority。公开 `Host` Authority
与可选浏览器 Origin 必须逐项精确配置。没有 `Origin` 的 Server-to-server
请求仍然有效。格式错误或重复的 Bearer Credential 会在 Body Decode 前失败。

## 已实现 Operation

| Capability | HTTP+JSON | JSON-RPC | Backend 要求 |
| --- | --- | --- | --- |
| Agent Card Discovery | 是 | 共享路由 | 不可变且已校验的 Snapshot |
| Send Message | 是 | 是 | 耐久 Message Idempotency 与 Task Projection |
| Stream Message | SSE | SSE | 已提交的有序 Event |
| Get/List Task | 是 | 是 | Tenant-scoped 稳定 Projection 与 Cursor |
| Cancel Task | 是 | 是 | 耐久请求与 Race-safe Lifecycle Transition |
| Subscribe Task | SSE | SSE | 先 Snapshot，再输出已提交有序 Event |
| Push Config CRUD | 是 | 是 | Secret 加密与 Authorization-first Lookup |
| Extended Agent Card | 是 | 是 | 已认证、Caller-scoped Projection |

按照 A2A ProtoJSON 模型要求，未知 JSON Field 会被忽略；未知 Enum、非法
Lifecycle 组合、非 Canonical Route、不支持的 Version、超限 Value/Collection、
非法 Media/URL 则 Fail Closed。REST Error 使用 AIP-193 结构与 Canonical HTTP
Status；JSON-RPC Error 使用 A2A 定义的 Code Mapping。

## 耐久 Service 合约

`A2aTaskService` 刻意保持 Storage-neutral。生产实现必须满足：

- Tenant 与 Subject 只能来自 `A2aRequestContext`；不能把 Body Tenant Override
  或 Agent Card Description 当作 Authority；
- 使用耐久 Caller/Message Identity 去重；Lost ACK 后返回原始已提交 Result；
- 把不透明 A2A Task/Context ID 映射到内部 Run，不泄露内部 ID，也不允许跨
  Tenant Lookup；
- 基于 Stable Snapshot 分页，并把 Continuation Token 绑定到 Tenant、Filter、
  Ordering 与 Snapshot Boundary；
- Stream 必须来自已提交 Journal/Outbox；Subscription 先发送 Snapshot，再发送
  新提交 Event；整个 Stream 生命周期都占用 Operation Permit；
- 报告 Cancellation 前先提交 Intent，并在耐久 Lifecycle Fence 下解决
  Completion 与 Cancellation Race；
- Push Credential 静态加密且不进入日志；Destination Policy 必须防 SSRF 与
  DNS Rebinding；使用 Transactional At-least-once Outbox、有限 Retry 与
  Dead-letter Policy；
- 基础设施处于不确定状态时返回 `Unavailable`，除非权威读取证明已提交结果。

仓库中的 TCK Fixture 刻意使用进程内内存与只允许 Loopback 的 Webhook。它只是
Compatibility Test Input，不是此耐久合约的实现。

## 组装 Server

实现四个 Policy/Application Trait，构造一个不可变 Agent Card，并且只在进程
启动时构建一次 Router：

```rust,ignore
let card = A2aAgentCard::builder(
    "orders-agent",
    "Creates and tracks authorized orders.",
    "1.4.2",
)?
.capabilities(
    A2aAgentCapabilities::new()
        .streaming(true)
        .push_notifications(true)
        .extended_agent_card(true),
)
.interface(A2aAgentInterface::new(
    "https://agents.example.com/a2a/rest",
    A2aBinding::HttpJson,
)?)?
.interface(A2aAgentInterface::new(
    "https://agents.example.com/a2a/jsonrpc",
    A2aBinding::JsonRpc,
)?)?
.default_input_modes(vec!["application/json".into()])?
.default_output_modes(vec!["application/json".into()])?
.skill(A2aAgentSkill::new(
    "create-order",
    "Create order",
    "Creates an order under local authorization policy.",
    vec!["orders".into()],
)?)?
.build()?;

let options = A2aServerHttpOptions::new()
    .with_allowed_authorities(["agents.example.com"])?
    .with_allowed_origins(["https://console.example.com"])?
    .with_limits(1_048_576, 1_024, 512)?
    .with_maximum_response_body_bytes(8_388_608)?
    .with_timeouts(Duration::from_secs(30), Duration::from_secs(60))?
    .with_maximum_stream_events(16_384)?
    .with_bearer_challenge("Bearer realm=\"stateknot-a2a\"")?;

let shutdown = CancellationToken::new();
let server = A2aServer::new(
    card,
    authenticator,
    authorizer,
    shared_admission,
    durable_task_service,
    options,
    shutdown.clone(),
)?;
let listener = TcpListener::bind("127.0.0.1:8080").await?;
axum::serve(listener, server.router())
    .with_graceful_shutdown(shutdown.cancelled_owned())
    .await?;
```

TLS 与可信 Proxy Termination 位于 Router 外层。Allowlist 应配置应用实际看到的
Authority；除非外层可信组件已经替换 Request Authority，否则不要信任客户端
提供的 Forwarding Header。

## 部署门禁

暴露流量前，必须验证：

- `A2aServerAuthenticator` 校验 Issuer、Audience、Resource、Expiry、
  Delegation 与 Revocation；
- Authorization 测试证明被拒绝的 Task/Config ID 不会被披露；
- 多 Replica 共享 Quota/Admission 的行为；
- 数据库支持的 Idempotency、Cancellation Race、Stream Replay、Retention、
  Push Encryption、Egress Policy、Retry 与 Dead-letter Recovery；
- Graceful Drain 足以完成已接纳 Unary Operation，终止后的 SSE Client 可恢复；
- 外部 TLS、去除 Secret 的 Request Log，以及 Overload、Auth Failure、Stream
  Expiry、Push Backlog 的 Metric、Trace 与告警阈值；
- 精确的 [A2A Conformance 门禁](a2a-conformance.zh-CN.md)，以及应用自己的
  Failure Injection 与 Restore Test。

## 不作出的声明

- A2A Client 行为或耐久 Outbound-agent Invocation；它们属于独立的
  [A2A Client Profile](a2a-client.zh-CN.md)；
- gRPC Transport；
- 内置 Identity Provider、Policy Engine、数据库 Task Service 或 Push
  Dispatcher；
- Stable API Compatibility、crates.io 发布或生产就绪的 StateKnot Release。
