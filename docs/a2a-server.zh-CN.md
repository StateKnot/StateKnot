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
Admission、持久化 Task Projection、Stream、Cancellation 与可靠 Push Delivery。

## Request Boundary

每个非 Discovery Request 都按以下顺序执行：

```text
shutdown / Host / Origin / canonical path / Content-Type / body ceiling
  -> 进程内 Request Admission
  -> Bearer Authentication（先于 Body Parsing）
  -> A2A Wire Decode 与有界 Contract Validation
  -> 基于 Decoded Operation 授权（先于 Task/Config Lookup）
  -> 调用方实现的 Replica-wide Quota Admission
  -> 可恢复 A2aTaskService Operation
  -> 有界 Response 或有界 SSE Stream
```

公开 Agent Card 位于 `/.well-known/agent-card.json`，支持 `Cache-Control`、
`ETag`、`Last-Modified` 与 Conditional Request。它是唯一不执行 Credential
Authentication 的路由。启动时，Card 声明的 Capability 和 Interface URL
必须与 Task Service 及挂载路径完全一致，否则构建失败。

边界不接受 Forwarded-host Shortcut 或通配 Authority。公开 `Host` Authority
与可选浏览器 Origin 必须逐项精确配置。没有 `Origin` 的 Server-to-server
请求仍然有效。格式错误或重复的 Bearer Credential 会在 Body Decode 前失败。

## 请求准备期限

`with_timeouts(request_timeout, stream_idle_timeout)` 同时约束初始请求准备。
认证成功后，Server 按单调时钟只采样一次期限，覆盖请求体收集、规范化、基于
已解码操作的授权和跨副本准入。授权与准入共享剩余预算，不会在进入下一个
Callback 时重新获得完整超时。认证保留原有独立预算；准备成功后，Backend
执行或 Stream 创建才开始各自原有的独立预算。

此期限适用于两种 Binding 的全部十一种已解码操作。请求体只在 SDK 实际读取
它的路径收集：REST Send/Stream 与 JSON-RPC 保留原有规范化；REST 创建
Push Config 在配置的 Body 上限内缓冲原始字节。其他路由不会新增 Body 读取。
Push Request 保留原始 Method、URI、HTTP Version、Header（含 Content-Length）
与 Extension 实例。高于 Axum 默认 2 MiB 的显式上限仍然有效。

请求体等待超时返回纯文本 HTTP 408 `request timed out`；观察到 Shutdown 时
返回纯文本 HTTP 503 `service unavailable`。准备 Callback 超时使用既有 A2A
Internal Error `A2A operation timed out`，Shutdown 使用
`A2A service is shutting down`，按各 Binding 的普通流前错误 Envelope 返回。
按时完成的策略拒绝与不可用保留原映射。REST Push 的 Body Buffer 失败保留
固定 SDK 完整的纯文本 400/413 与允许 Origin 的响应头；REST Send、JSON-RPC
保留各自原有早期 Body 错误映射。多个缺陷同时存在的输入会在 Backend 派发前
拒绝，但其错误优先级不作为兼容保证。

边界在等待后重新检查真实时钟和 Shutdown。迟到的授权结果不能开始准入，
准备失败不能调用 Backend。请求拥有并释放正在等待的工作，不创建后台 Policy
Task。等待 Policy 时不占 Operation Permit；最后一道检查可能在取得 Permit
后立即观察到期限或 Shutdown，此时会在 Backend 前释放。准备错误 Response
返回前释放 Request 容量。

宿主认证、策略与准入 Future 必须有界且不阻塞。Rust 无法抢占同步阻塞的
Callback 或 Decode；准备阶段在控制权返回后拒绝迟到的有效规范化、已解码授权
或准入结果。这不承诺硬性
墙钟响应时限、外部配额回滚、即时发现客户端断开或持久化 Task Cancellation。
下述可选订阅复核仍使用每次步骤独立的 Idle 预算。

[限定执行记录](qualification/experimental-a2a-preparation-deadline-2026-10-16.zh-CN.md)
保留原累计期限失败、源码/缓存绑定问题、夹具修正与最终本地门禁。这些检查不
授予部署身份提供方、持久化 Task Backend、完整 CI 或 R1 资格。

## 已实现 Operation

| Capability | HTTP+JSON | JSON-RPC | Backend 要求 |
| --- | --- | --- | --- |
| Agent Card Discovery | 是 | 共享路由 | 不可变且已校验的 Snapshot |
| Send Message | 是 | 是 | 持久化 Message Idempotency 与 Task Projection |
| Stream Message | SSE | SSE | 已提交的有序 Event |
| Get/List Task | 是 | 是 | Tenant-scoped 稳定 Projection 与 Cursor |
| Cancel Task | 是 | 是 | 持久化请求与 Race-safe Lifecycle Transition |
| Subscribe Task | SSE | SSE | 先 Snapshot，再输出已提交有序 Event |
| Push Config CRUD | 是 | 是 | Secret 加密与 Authorization-first Lookup |
| Extended Agent Card | 是 | 是 | 已认证、Caller-scoped Projection |

按照 A2A ProtoJSON 模型要求，未知 JSON Field 会被忽略；未知 Enum、非法
Lifecycle 组合、非 Canonical Route、不支持的 Version、超限 Value/Collection、
非法 Media/URL 则 Fail Closed。REST Error 使用 AIP-193 结构与 Canonical HTTP
Status；JSON-RPC Error 使用 A2A 定义的 Code Mapping。

## 可恢复 Service 合约

`A2aTaskService` 刻意保持 Storage-neutral。生产实现必须满足：

- Tenant 与 Subject 只能来自 `A2aRequestContext`；不能把 Body Tenant Override
  或 Agent Card Description 当作 Authority；
- 使用持久化 Caller/Message Identity 去重；Lost ACK 后返回原始已提交 Result；
- 把不透明 A2A Task/Context ID 映射到内部 Run，不泄露内部 ID，也不允许跨
  Tenant Lookup；
- 基于 Stable Snapshot 分页，并把 Continuation Token 绑定到 Tenant、Filter、
  Ordering 与 Snapshot Boundary；
- Stream 必须来自已提交 Journal/Outbox；Subscription 先发送 Snapshot，再发送
  新提交 Event；整个 Stream 生命周期都占用 Operation Permit；
- 报告 Cancellation 前先提交 Intent，并在持久执行 Lifecycle Fence 下解决
  Completion 与 Cancellation Race；
- Push Credential 静态加密且不进入日志；Destination Policy 必须防 SSRF 与
  DNS Rebinding；使用 Transactional At-least-once Outbox、有限 Retry 与
  Dead-letter Policy；
- 基础设施处于不确定状态时返回 `Unavailable`，除非权威读取证明已提交结果。

仓库中的 TCK Fixture 刻意使用进程内内存与只允许 Loopback 的 Webhook。它只是
Compatibility Test Input，不是此持久执行合约的实现。

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

## 实验性订阅当前策略复核

对只读 `SubscribeToTask` SSE，在上方组装示例把 Options 传给
`A2aServer::new` 前显式启用：

```rust,ignore
let options = options.with_subscription_authorization_rechecks();
```

默认关闭。此 consuming 方法幂等，Clone 和其他 Options 方法会保留开关与原有
数值限额。它不会开启缺失的 Streaming Capability，也不改变
`SendStreamingMessage`、Unary、原有 Admission、Retry 或持久化 Task Cancellation。

原 Authentication、Decoded Authorization、Admission 和 Backend Subscription
创建完成后，每个被 poll 的 next-step 在 poll Backend 前，以及取得正常 Event 后，
调用现有 `A2aServerAuthorizer`。两次检查都保留最初完整 Principal、Canonical
Scopes、`SubscribeToTask`、原解码 Task ID 和空 Config ID。宿主必须查询当前权威
Tenant/Resource Policy 并拒绝失效绑定；恒定允许的 Authorizer 不会因为打开开关
而获得撤销能力。Adapter 不保留 Bearer 做重新认证，也不自行重新校验 Credential
Expiry 或 Revocation。

每一步只建立一个原 Stream-idle 绝对期限，覆盖 Pre-check、Backend 等待和
Post-check。每次 Callback 的期限还取该 Idle Deadline 与本阶段开始加原
Request Timeout 的较早值；await 后实际检查时钟与 Shutdown，阻止晚到 Event
交付。宿主 Callback 必须有界且不阻塞：Rust Timeout 无法抢占同步阻塞工作，
事后时钟检查不会把此类 Callback 变成具有有界延迟的实现。

被 poll 的步骤观察到拒绝、不可用、超时、Shutdown、Backend Error/EOF 或 Event
Cap 耗尽时，会在返回终结输出前 drop 自有 Backend Stream 和 Operation Permit。
获准 Terminal Event 先通过 Post-check，再于交付前退役这些资源。Headers 已提交
后，错误沿用 SDK 原有 REST/RPC Stream Envelope，不改变 HTTP Status。
Pre-check 拒绝发生在原 Backend 创建之后，不能据此声称从未创建 Backend。

最后成功的 Post-check 是该 Event 的策略线性化点，不是与 TCP 发送原子化的撤销
边界。已经交给 SDK、Transport 或 Proxy 的字节无法收回。未被 poll 的 next-step
不会启动后台策略任务、周期复核或到期计时器。真实连接清理依赖 HTTP 栈观察关闭
并 drop Body；Rust Stream Drop 不代表 Durable Task 已取消或 Backend 已清理。
下方更广的部署门禁仍然必需。

冻结的 v5 共 930 文件，在原 opt0/default stack 下六项本地门禁通过：100 个
Workspace Suite，2,108 passed、0 failed、6 个原有 ignored；10 组 Doctest 共
48 passed。包含 8 个 Direct Ownership Test，以及一个真实 TCP Family 经
REST/RPC 两条 Binding 执行 17 个矩阵 ID；这是可控 Policy/Stream 证据，不是
34 个独立 Test Family。Fmt/Clippy 在首轮 v5 通过，其余四项在源码不变、获得
Loopback 授权后通过。原 Fixture/Format 红与 Sandbox Listener Denial 保留于
[限定执行记录](qualification/experimental-a2a-subscription-policy-2026-10-16.zh-CN.md)。
这不新增 Native 数据库、独立 Server TCK、完整 CI、部署中的 Identity/Policy
Backend、Durable A2A Backend 或 R1 资格。

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

- A2A Client 行为或可恢复 Outbound-agent Invocation；它们属于独立的
  [A2A Client Profile](a2a-client.zh-CN.md)；
- gRPC Transport；
- 内置 Identity Provider、Policy Engine、数据库 Task Service 或 Push
  Dispatcher；
- Stable API Compatibility、crates.io 发布或生产就绪的 StateKnot Release。
