<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# AgentService v1

`AgentServiceV1` 是已经实现、带版本的耐久 Agent 嵌入式服务边界，负责提交、完整性校验读取与两阶段取消。它是建立在 PostgreSQL 和精确可执行注册表之上的 Library Service；它不是 HTTP、gRPC 或 SSE Server，也不负责认证 Transport Credential。

StateKnot 仍处于 pre-alpha。该 API 已有可执行证据，但尚无稳定性、Crate 发布或生产支持承诺。

## 已实现合约

- 每个操作都接收由已验证 Credential 派生的 `AgentServiceCaller`；
- 强制使用 `AgentServiceAuthorizer`；提交、读取与取消均先授权，再披露 Deployment 或耐久目标是否存在；
- `AgentServiceRegistryBuilder` 最多冻结 4,096 个精确 Agent Revision，并拒绝重复 Identity、Schema 不一致与 Deployment Drift；
- 提交使用 Tenant-scoped `AgentSubmissionKey` 进入 `DurableAgentRuns`，不会在 Service Call 内联启动 Model 或 Tool；
- Timeout 或 Lost ACK 后，以相同逻辑 Submission Key 与内容重试会恢复原 Run；内容变化会 Fail Closed 为 Conflict；
- Run 与 Submission-key 查询返回经过完整重新校验的 Public Snapshot；
- 取消会绑定调用方持久保留的两个 `AgentCancellationIds`，记录 PostgreSQL 权威时钟 Observation 与不可变 Policy Decision Digest，并返回 `Committed` 或 `Idempotent`；
- 取消 Waiting Run 时，会在同一事务内 Abandon 所有未完成 Interrupt 与 Timer；Worker 随后依据耐久证据确认最终取消。

Service Control Event 刻意不保存 Caller Input、Principal 文本、Policy Payload、Secret 与 Failure Message。公开 Schema 位于 [`agent-service-control-event/1.0.0`](https://stknot.com/schemas/runtime/agent-service-control-event/1.0.0)。

## 启动绑定

构建 Service 前，必须注册全部 Graph、Reducer、Node、类型化 Input/Output Schema 与标准 Runtime Schema。Executable Registry 必须同时包含 Agent Admission Schema 与 Agent Service Control Schema。

```rust
use std::sync::Arc;
use stateknot_runtime::{
    AgentServiceRegistryBuilder, AgentServiceV1,
    register_standard_agent_service_control_event_schema,
};

register_standard_agent_service_control_event_schema(&mut schema_builder)?;

let executable_registry = executable_builder.build()?;
let mut deployments = AgentServiceRegistryBuilder::new();
deployments.register(Arc::new(provider_native_definition.clone()))?;

let service = AgentServiceV1::new(
    store.clone(),
    executable_registry,
    deployments.build(),
    Arc::new(authorizer),
)?;
```

`AgentServiceDeployment` 是接入其他预编译 Agent 形态的扩展点。Descriptor 与 Compiled Graph 会在启动时冻结快照；实现生成的 Initial State 必须匹配 Graph State Schema。

## 提交、读取与取消

```rust
let caller = AgentServiceCaller::new(tenant_id, authenticated_principal);

let admitted = service
    .submit(
        caller.clone(),
        &submission_key,
        &agent_identity,
        request,
    )
    .await?;
let run_id = admitted.snapshot().provenance().run_id();

let snapshot = service.load(caller.clone(), run_id).await?;
let same_run = service.load_by_key(caller.clone(), &submission_key).await?;

// 首次调用前先在 Ingress 持久保存这组 Identity。Timeout 后必须复用完全
// 相同的一对；重新生成会形成竞争 Cancellation Request。
let cancellation_ids = AgentCancellationIds::generate();
let outcome = service
    .request_cancellation(caller, run_id, cancellation_ids)
    .await?;
```

提交在耐久 Admission 完成后返回。Scheduler 与 Agent Worker 必须在独立角色中 Claim 并执行 Run。取消也分成两个耐久阶段：Service 记录请求；只有当 Model Usage 与 External-effect Evidence 足够时，Agent Loop 才确认 Terminal Cancelled Outcome。

## 重试规则

| 操作 | 安全恢复动作 |
| --- | --- |
| `submit` Timeout | 使用相同 Tenant、Caller、Agent Identity、Submission Key 与逻辑 Request 重试。 |
| `load` / `load_by_key` Timeout | 重试相同的已授权读取。 |
| `request_cancellation` Timeout | 使用相同 `run_id` 与完全相同的 `AgentCancellationIds` 重试。 |
| 取消时换用了新 IDs | 将 Conflict 视为竞争请求，不得伪装成幂等成功。 |
| Run 为 `cancellation_requested` | 停止新 Dispatch，让 Driver/Lifecycle Reconciliation 证明最终状态。 |

嵌入 Transport 不得原样暴露内部 Registry 或 Database Error。应将闭合的 `AgentServiceError` 映射为有界 Public Error Model，在可信日志中保留 Correlation ID，并保持“授权先于 Not Found”的顺序。

## 生产集成责任

嵌入服务仍负责：

1. TLS 终止与 Token/mTLS 校验；
2. 从已验证 Credential 派生 `TenantId` 和 `PrincipalIdentity`，不得信任 Request Body 中的 Identity Claim；
3. 使用版本化 Policy 与可保留 Decision Evidence 实现 `AgentServiceAuthorizer`；
4. 调用 Facade 前耐久保存 Submission 与 Cancellation Identity；
5. Scheduler/Worker Role、Graceful Drain、Health/Readiness、Metrics、Tracing、Rate Limit 与 Overload Control；
6. Public Error Mapping、Secret Redaction、Backup/Restore 与 Tenant Isolation 验证。

不得把远程 Policy 调用放进数据库事务。如果 Policy 位于远端，应先在专用耐久 Ledger 中提交或加载有界 Decision Evidence，再由同步 Facade 消费该可信快照。

## 可执行证据

配置 PostgreSQL 16 或 17 测试数据库后，强制运行 Integration Test，不允许因基础设施缺失而跳过：

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
cargo test -p stateknot-runtime --test postgres \
  agent_service_authorizes_submits_recovers_and_cancels_without_redispatch \
  --locked
```

该证据覆盖：Authorization-first 的 Missing-resource 处理、精确 Submission Recovery、Key-based Read、Cancellation Commit/Retry/Conflict，以及 Service Call 自身零 Model/Tool Dispatch。

## 明确排除

- 尚无稳定 HTTP/gRPC/SSE Wire API；
- 不内置 Identity Provider，也不提供默认 Allow-all Authorizer；
- PostgreSQL 不可用时不会降级为 In-memory；
- 不提供隐式“提交并等待”操作；
- External Effect 或 Usage 未被证明时，不宣称 Terminal Cancellation；
- pre-alpha 阶段不承诺 API 稳定或生产就绪。
