<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# A2A 1.0 Client 与耐久 Remote-agent Profile

> 状态：已实现的 Pre-alpha Profile；Rust API 尚不稳定。<br>
> Binding：HTTP+JSON 与 JSON-RPC 2.0，包含 SSE。<br>
> 明确排除：gRPC、自动信任公网 Agent、自动重试 Send、没有运维证明的自动
> Reconciliation、API-key/Basic/mTLS Security Profile，以及官方 Client
> Conformance 声明。

本 Profile 有两个刻意分离的层次：

- `A2aClient` 是严格、Stateless 的 A2A 1.0 Client，实现全部 11 个 Operation；
- `A2aRemoteAgent` 把一个已发现 Skill 固定为 StateKnot `ErasedTool`，由
  `DurableInvocationExecutor` 与 PostgreSQL Invocation Ledger 独占 Dispatch 和
  Recovery 决策权。

直接的 Read/List/Stream 管理操作可以使用 Client；Graph 内的消息提交必须通过
耐久 Adapter。若可恢复 Graph 直接调用 `send_message`，就会绕过
Durable-before-dispatch Record，不属于生产支持的组合方式。

## 已实现的 Operation Surface

| Operation | Interface 下的 HTTP+JSON Route | JSON-RPC Method | Result |
| --- | --- | --- | --- |
| Send Message | `POST message:send` | `SendMessage` | Task 或 Message |
| Stream Message | `POST message:stream` | `SendStreamingMessage` | 有序 SSE Event |
| Get Task | `GET tasks/{id}` | `GetTask` | Task |
| List Tasks | `GET tasks` | `ListTasks` | 有界 Task Page |
| Cancel Task | `POST tasks/{id}:cancel` | `CancelTask` | Task |
| Subscribe Task | `POST tasks/{id}:subscribe` | `SubscribeToTask` | 有序 SSE Event |
| Create Push Config | `POST tasks/{id}/pushNotificationConfigs` | `CreateTaskPushNotificationConfig` | Push Config |
| Get Push Config | `GET tasks/{id}/pushNotificationConfigs/{config}` | `GetTaskPushNotificationConfig` | Push Config |
| List Push Configs | `GET tasks/{id}/pushNotificationConfigs` | `ListTaskPushNotificationConfigs` | 有界 Page |
| Delete Push Config | `DELETE tasks/{id}/pushNotificationConfigs/{config}` | `DeleteTaskPushNotificationConfig` | Empty Result |
| Extended Agent Card | `GET extendedAgentCard` | `GetExtendedAgentCard` | Agent Card |

每个请求都携带 `A2A-Version: 1.0`，协商后的 Extension URI 按配置顺序写入
`A2A-Extensions`。若选中 Interface 声明 Tenant，HTTP+JSON 会把它放在第一个
Path Segment，并在 Protocol Request Model 定义 Tenant 时写入该字段；JSON-RPC
则写入 `params`。

SSE 只接受标准 Message/Error Event。首个成功 Event 必须是一个 Task，或 Streaming
Send 唯一的 Message；Task 之后只能出现 Status/Artifact Update。每个 Update 必须保留
完全相同的 Task/Context Identity；Artifact Append 必须引用已建立且未封口的 Artifact
ID；Terminal 或 Interrupted State 后不得再出现 Event。每个 Unary Response 与 Stream
Event 都会校验 JSON-RPC Version、Request ID，以及 `result`/`error` 的互斥存在性；
合法的 `result: null` 不会与缺失 Result 混淆。Duplicate JSON Key、非法 Union、
Cross-resource Response、超限数据、提前终止或 Idle Stream 全部 Fail Closed。

Unary Send 还会验证 Execution Mode：除非 `returnImmediately` 为 true，Task Response
必须已经 Terminal 或 Interrupted。Task Page 会验证请求 Filter、包含等号的
`statusTimestampAfter` 边界、Task ID 唯一性、按最新 Status 排序、Artifact Projection
规则与 Response Page Size。只有 HTTP Status、`google.rpc.Status` 与官方 `ErrorInfo`
Identity 三者一致时，HTTP+JSON Error 才会暴露权威 A2A Code。

## 在执行前冻结 Discovery

`A2aClient::discover` 只执行一次有界 Public Agent Card Exchange，然后冻结结果：

1. 生产只允许 HTTPS；只有显式 Test/Sidecar Constructor 可使用 Literal Loopback
   HTTP；
2. 拒绝 URL Credential、Query、Fragment、Redirect 与隐式 Retry；
3. 使用 StateKnot 自有的有界 Contract 校验 Agent Card；
4. 按 Server Preference 选取首个受支持的 A2A 1.0 HTTP Interface；
5. 要求该 Binding 和 URL 与本地 Egress Allowlist 精确相等；
6. 校验所有选中或 Required Extension；
7. 校验配置的 Anonymous 或完整、单 Scheme Bearer-compatible Security Alternative
   能满足 Agent Card。

当前已认证 Profile 支持通过 Bearer Token 承载的 HTTP Bearer、OAuth 2.0 与 OpenID
Connect 声明。若 Agent Card 要求 API Key、Basic、mTLS，或一个包含多个 Scheme 的
AND Group，Client 会拒绝，而不会只完成部分认证。

若 DNS、证书与 TLS 运维可信，使用 TLS Server Identity 与精确 Interface Pin。若
Agent Card 通过独立流程供应，再用 `CanonicalSha256` 校验其 RFC 8785 Canonical
Digest。Card 内嵌 Signature 会被解析为数据；没有应用自有 Signature Policy 时，
它不会自动成为 Trust Anchor。

```rust,no_run
use std::sync::Arc;
use stateknot_integrations::{
    A2aAgentCardEndpoint, A2aAgentCardTrust, A2aBinding, A2aClient,
    A2aClientInterfacePin, A2aClientOptions, A2aClientSecurity,
};

let client = A2aClient::discover(
    A2aAgentCardEndpoint::https(
        "https://partner.example/.well-known/agent-card.json",
    )?,
    vec![A2aClientInterfacePin::https(
        "https://partner.example/a2a",
        A2aBinding::HttpJson,
    )?],
    A2aAgentCardTrust::TlsServerIdentity,
    A2aClientSecurity::bearer("oidc", Arc::new(token_provider))?,
    vec![],
    A2aClientOptions::default(),
).await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Token Provider 会为每个已认证 Operation 执行一次。Request 包含 Card Digest、
Binding、Operation、Remote Routing Tenant、Target Task ID、Card 所需 Scope；通过
`A2aRemoteAgent` 调用时还包含本地 Tenant、Run、Invocation 与 Physical Attempt ID。
生产 Provider 应针对该精确 Tuple 签发或读取短期 Token。Token、Remote Response
Text 与 Response Body 不会进入 Public Error 或 `Debug` 输出。

Anonymous Client 不会 Dispatch `GetExtendedAgentCard`。返回值会被限制大小并重新校验，
但只作为数据返回：它不会修改 Immutable Client、改变 Egress/Security Pin，或静默重绑
Tool。要采用 Extended Card，必须先审核，再构造新的 Pinned Client/Binding。

## 将 Remote Skill 绑定到耐久执行

Agent Card 提供 Media、Security 与描述性 Skill Metadata，但 A2A 1.0 既没有 Skill
Argument JSON Schema，也没有在 Send Request 中路由到 Skill 的字段。因此：

- `skill_id` 只固定 Discovery/Security/Media 证据；
- StateKnot 把校验后的 Tool Input 映射为一个 `application/json` Data Part；
- 接收端 Agent 根据 User Message 决定行为；
- 可信本地 `A2aSchemaRegistry` 是 Input 和 Output 的权威；
- Output 是标准的单 Key `{"message": ...}` 或 `{"task": ...}` Projection，并再次
  使用固定的本地 Output Schema 校验。

```rust,no_run
use std::sync::Arc;
use stateknot_core::DurationMillis;
use stateknot_integrations::{
    A2aRemoteAgent, A2aRemoteAgentDelivery, A2aRemoteAgentRecovery,
};
use stateknot_runtime::ToolProviderRegistryBuilder;

let recovery = A2aRemoteAgentRecovery::operator_attested_context_task_history(
    4,
    64,
    DurationMillis::new(1_000)?,
)?;
let remote = A2aRemoteAgent::bind_with_recovery(
    descriptor,
    client,
    "answer",
    A2aRemoteAgentDelivery::AtMostOnce,
    recovery,
    schemas,
)?;

let mut tools = ToolProviderRegistryBuilder::new();
tools.register(Arc::new(remote))?;
let tools = tools.build();
# Ok::<(), Box<dyn std::error::Error>>(())
```

只有 Descriptor 与真实行为完全一致时 Binding 才会成功：Network Read/Write、无
Filesystem 与 Dynamic Code、Cooperative Cancellation、无 Progress Event、Credential
Requirement 一致、Status-query 声明与 Recovery Policy 一致、不声明 Compensation，
并且两个本地 Schema 都存在。使用 `A2aRemoteAgent::bind` 可保持 Fail-closed 的
Disabled Recovery；对应 Descriptor 不得声明 Status-query Support。

## Delivery 与 Recovery Contract

A2A 1.0 不保证接收方对 `messageId` 去重。必须选择一个真实的部署属性，并在
`ToolDescriptor` 中编码同样的事实：

| Delivery | Message ID | 必需 Descriptor Semantics | Recovery Rule |
| --- | --- | --- | --- |
| `AtMostOnce` | `stateknot-attempt-{attempt_id}` | Non-idempotent Write + Unsupported | 不重复不确定的 Physical Attempt |
| `MessageIdDeduplicated` | `stateknot-invocation-{idempotency_key}` | Idempotent Write + Required Key | 只有远端耐久去重已有运维证据时才安全 |

`MessageIdDeduplicated` 是 Operator Assertion，不是从 Agent Card 推导的能力。远端
必须在本地 Invocation-ledger Retention 与灾备窗口的完整周期内，耐久保存并去重该
ID。启用前需记录 Remote Key Scope、Conflict Behavior、Retention、Replica
Consistency、Backup Behavior 与验证证据。

自动 Reconciliation 由 `A2aRemoteAgentRecovery` 单独选择：

| Recovery Mode | 必需的运维证明 | `Unknown` 时的 Provider 行为 |
| --- | --- | --- |
| `Disabled` | 无 | 不执行 Provider I/O；留给已授权的人工 Reconciler |
| `ContextTaskHistory` | Peer 在完整 Recovery Window 内保留 Client 提供的 Context ID，提供完整且稳定的 `ListTasks` Pagination，并在 Task History 中保留原始 User `messageId` | 最多查询 1–16 页、每页最多 100 个 Task、每个 Task 取 1–256 条 History；绝不重新 Send |
| `MessageIdReplay` | Peer 在完整 Recovery/Retention Window 内对精确 `messageId` 做耐久去重 | 重放精确的原始 Request Identity；只允许与 `MessageIdDeduplicated` Delivery 一起启用 |

两个 Enabled Mode 都要求 Descriptor 声明 Status-query Support，并配置 1 ms–1 h
的正数耐久 Poll Interval。`ContextTaskHistory` 会在首次 Message 上附加单向、不透明的
Context ID。Probe 只接受唯一匹配的 Task History，并要求其中的 Role、Context 与
Payload 等于原始 Message（Server 只能补充该 Task 的 ID）；没有匹配返回 `Pending`，
重复 Task ID、多个 Message Match、同 ID Payload Substitution、非法 Pagination 或扫描
超过配置上限都会 Fail Closed。该 Context 不暴露原始 Tenant、Run、Thread、Invocation
或 Attempt ID。

耐久执行顺序如下：

```text
PostgreSQL prepared/executing revision
  -> exact physical attempt and message ID
  -> one A2A send, with no client retry
  -> validate remote task/message and local output schema
  -> PostgreSQL committed/failed/unknown terminal revision
```

所有 Adapter 都会显式设置 `returnImmediately: true`。标准 `bind` 与
`bind_with_recovery` 路径只投影合法 Task Response，不声称远端 Task 已进入 A2A
终态；等待仍由应用自有、独立授权的 `get_task` 或 `subscribe_to_task` 负责。

`bind_with_durable_artifacts` 是明确的耐久完成 Profile，并且只接受 Task Response。
非终态 Task 会返回 `Unknown`，同时携带绑定 Card/Interface/Endpoint 的
`protocol.a2a.task` Recovery Handle。Provider 之后只会针对该精确 Task 发起直接授权的
`GetTask`，绝不重发原始业务 Message。进入 `Completed` 后，每个有界 Terminal Part
都会通过配置的 Artifact Sink 写入；Failure、Cancellation、Rejection 与未知远端状态
继续作为 Terminal Error。该 Profile 还要求精确 Origin Event、已启用 Provider Recovery
以及正数 Tool Artifact Capacity。Tool Output 只包含有界
`{kind, task_id, context_id, state, artifact_count}` Projection；物化后的值位于
`ToolArtifacts`。PostgreSQL 与 Object Storage 不变量见
[耐久 Artifact Storage 指南](artifact-storage.zh-CN.md)。

可能 Dispatch 前的 Cancellation/Deadline 是 `NotStarted`。Dispatch 可能开始之后，
Timeout、Cancellation、Lost Connection、Invalid Response、HTTP Ambiguity 或非权威
Remote Error 都变成 `Unknown + ReconcileFirst`，StateKnot 不会再次发送。只有明确在
应用前拒绝 Request 的 Protocol Error——Parse Error、Invalid Request/Params、
Method/Operation/Content/Extension/Version Unsupported——才变成
`NotApplied + Never`。

在 Provider-native Agent 路径上，Enabled Binding 会把 `Unknown` 转换为一次受当前
Fence 与原 Physical Attempt Identity 约束的有界 Probe。权威 Result/Error Evidence
会原子提交到现有 Invocation Ledger；`Pending` 不修改 Invocation Evidence，而是形成
耐久 `SafeAfter` Node Retry。后续 Lease 再次 Probe；除非明确启用了经过证明的
`MessageIdReplay`，否则绝不会重复 Business Send。未启用 Provider 的 Binding 继续
保持人工 Fail-closed 路径。

## 默认 Resource Policy

| Limit | 默认值 | Hard Ceiling |
| --- | ---: | ---: |
| Connect Timeout | 10 s | Transport Policy |
| Discovery Timeout | 15 s | 15 min |
| Unary / Stream-establishment Deadline | 60 s | 15 min |
| Stream Idle Timeout | 60 s | 15 min |
| Request Body | 16 MiB | 32 MiB |
| Unary Response Body | 2 MiB | 2 MiB |
| SSE Line / Event / Total | 512 KiB / 2 MiB / 64 MiB | 2 MiB / 2 MiB / 72 MiB |
| SSE Event 数 | 4,096 | 65,536 |
| Task Page | 默认 50 | 100（A2A 1.0） |
| Push-config Page | 默认 50 | 256（本地上限） |

StateKnot Protocol Contract 还有更低的结构上限，包括 1 MiB Agent Card/Data Part、
每个 Message 或 Artifact 最多 128 Part、最多 256 条 History Message，以及最多 128
个 Task Artifact。应按真实业务配置更低上限，不要为了接收无界 Peer 而盲目放大。

## 生产门禁清单

- 维护精确 Destination/Binding Allowlist，并在应用校验之外独立限制 DNS、Proxy 与
  Sidecar Egress。
- 使用验证过的 TLS；不要跨 Host 或 Trust Boundary 使用 Loopback HTTP。
- Rollout 前固定并审核 Agent Card 变更、Skill Media、Scope、Extension、Tenant 与
  Security Alternative。
- 使用 Attempt-scoped Secret Manager 或 Workload Identity Token Provider；Bearer
  Token 不能写进配置文件、URL、Metadata、Log 或 Trace。
- 只通过 Durable Invocation Executor 注册 Adapter；PostgreSQL Retention 必须长于
  所有 Remote Deduplication/Reconciliation Window。
- 每项 Recovery Attestation 都要记录 Owner、Review Date、Evidence Artifact、精确 Peer
  Deployment、Retention Window、Pagination Consistency 与 Rollback Procedure；任一
  声明发生 Drift 时必须关闭 Recovery。
- 对 `Unknown`、Reconciliation Backlog、Card-digest Drift、非法 Remote Contract、
  Authorization Denial/Unavailable、Stream Limit 与 Deadline Exhaustion 告警，但不记录
  Payload 或 Credential。
- 生产流量前测试 Accepted-request/Lost-response、Cancellation Race、Malformed
  JSON/SSE、Stale Card、Wrong Interface、Remote Deduplication、PostgreSQL Failover
  与 Restore。

## 可执行证据与声明边界

```console
cargo test -p stateknot-integrations --test a2a_client_contract --locked

STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='postgres://...' \
cargo test -p stateknot-integrations --test mcp_durable \
  a2a_send_is_durable_before_dispatch_and_unknown_is_not_redispatched \
  --locked -- --test-threads=1

STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='postgres://...' \
cargo test -p stateknot-runtime --test postgres \
  provider_native_graph_reconciles_unknown_tool_without_repeating_business_io \
  --locked -- --test-threads=1
```

Loopback Suite 在 HTTP+JSON 和 JSON-RPC 两种 Binding 上执行全部 11 个 Operation、
两个 SSE Surface、Tenant/Header Mapping、Attempt-scoped Authorization、Card/Interface
Drift、严格 Error、Stream Bound、Lost Response、无需重发的 Context/History Recovery，
以及经过证明的精确 Message Replay。PostgreSQL Evidence 证明 Request Dispatch 前已经
存在 Executing Revision，Lost Response 会提交为 `Unknown`，普通 Recovery 不会发送
第二条 Message。Provider-native Test 证明 `Unknown -> Pending ->` 耐久延迟 Retry
`-> Committed`，期间只有一次 Business Call 和两次有界 Probe。

这些是实现证据，不是官方 A2A Client 认证。冻结的官方 TCK 证据目前只适用于独立的
[A2A Server Profile](a2a-server.zh-CN.md)。Stable API Review、Live-partner
对两种 Recovery Attestation 的 Qualification、gRPC，以及生产耐久 Server-side
Task/Push Store 仍是独立 Release Gate。
