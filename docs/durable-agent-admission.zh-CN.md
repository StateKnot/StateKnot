<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 耐久 Agent Admission

状态：已实现的 pre-alpha 集成契约。Crate 尚未发布；公开耐久 Run/Result Facade
已经实现，但还没有兼容性承诺。

本文定义把一个已经认证且通过 Schema 校验的 Agent Request 转换为
Scheduler-visible 耐久工作的可信边界，覆盖 Core Admission Snapshot、
`DurableAgentAdmission` Runtime Facade、PostgreSQL Migration 15、精确重试与运维要求。
耐久 Ingress-key Routing 与 Terminal Result Read 由单独的
[公开 Run/Result 契约](durable-agent-runs.zh-CN.md)定义。认证、Policy Evaluation 与
预置 Provider-native Model/Tool Graph 仍不属于本 Admission 切片。

英文版见 [Durable Agent admission](durable-agent-admission.md)。

## 一个事务提交哪些事实

一次成功的 `DurableAgentAdmission::admit` 会在一个 PostgreSQL Transaction
中提交以下全部事实：

1. 不可变 Agent Descriptor、类型化 Request、按确定顺序排列的 Policy Budget
   Layer、解析后的有限 Budget、Graph Reference、已认证 Principal、授权 Scope 与
   Authorization Evidence；
2. 来自数据库时钟的 `admitted_at` 观测与 Domain-separated Admission Digest；
3. Run Lifecycle 从 Pending 到 Active 的状态边；
4. Sequence-one `agent-admitted` Control-plane Event；
5. Superstep-zero Checkpoint、精确 Initial State 与 Graph Entry Ready Set；
6. 让 Run 可执行的 Journal、Checkpoint、Lifecycle、Graph 与 Scheduler Projection。

Scheduler Query 无法观察这些写入之间的中间状态。Migration 15 还要求
`runs_scheduler_ready` 中的 Run 必须具有非空 Checkpoint；因此旧的低层
`PostgresStore::admit_run` 只能创建可按 ID 查询、但对 Scheduler 不可见的 Bootstrap
Row。新的 Agent 集成必须使用原子接口。

## 可信入口的职责

Admission 有意从外部认证与 Policy Evaluation 之后开始。构造
`AgentAdmissionAuthority` 前，嵌入 StateKnot 的 Control Plane 必须：

- 通过应用拥有的机制认证 Tenant 与 `PrincipalIdentity`；
- 解析一份不可变的 Agent、Graph、Policy、Schema、Model 与 Tool Deployment
  Snapshot；
- 对固定版本的 Policy 求值，收窄授权 Scope，并保存带 Schema 的 Allow Decision
  Evidence；
- 从 System、Tenant、Policy、Agent 与 Request Layer 推导 Budget，禁止调用方注入
  已解析的总量；
- 在完整 Ingress Retry Window 内保留不可变 Policy、Budget、Agent、Graph、
  Authorization 与 Initial State 输入。

`AgentAdmissionAuthority` 是审计快照，不是签名验证器。`Digest` 只能证明字节身份，
不能认证 Principal，也不会让其中的数据自动保密。

## 组装不可变 Deployment

冻结 Executable Registry 前，必须注册标准 Admission Event Schema 与全部应用
Schema。本地必须已经安装精确的 Graph、Reducer 与 Node 闭包，Compiled Graph 也必须
先按 Tenant 注册到 PostgreSQL。

```rust,no_run
let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
register_standard_agent_admission_event_schema(&mut schemas)?;
register_standard_graph_driver_event_schema(&mut schemas)?;
register_standard_graph_lifecycle_event_schema(&mut schemas)?;
register_application_schemas(&mut schemas)?;

let mut executables = ExecutableGraphRegistryBuilder::new(schemas.build()?);
register_release_graphs_reducers_and_nodes(&mut executables)?;
let executables = executables.build()?;

store
    .register_graph_definition(tenant_id.clone(), compiled_graph)
    .await?;

let admission = DurableAgentAdmission::new(store.clone(), executables)?;
```

Release 拥有的 Public-safe Event Schema 发布在
`https://stknot.com/schemas/runtime/agent-admission-event/1.0.0`。必须通过 Runtime
Helper 注册，不能手工写死 Digest。

## 构造并保留精确低层 Request

本节描述 `DurableAgentAdmission::admit` 这一 Exact-ID 低层边界。面向用户的 Ingress
通常应使用 `DurableAgentRuns::submit`：它在同一 Transaction 内持久化
Tenant-scoped Key Mapping，并允许 Retry 使用新的 Candidate ID。详见
[Run/Result 指南](durable-agent-runs.zh-CN.md)。

完整 ID Bundle 只能分配一次。调用 `admit` 前，先把它与已认证的外部 Idempotency
Key 关联并持久化；发生 Timeout 后绝不能生成替代 ID。

```rust,no_run
let ids = AgentRunIds::generate();

// 先在可信入口存储 external_idempotency_key -> ids。
let request = DurableAgentAdmissionRequest::new(
    tenant_id,
    ids,
    agent_descriptor,
    agent_request,
    evaluated_budget_layers,
    graph_reference,
    admission_authority,
    initial_state,
)?;

// 保存这些已校验 Bytes，用于结果不明时的精确重试。
let retry_bytes = serde_json::to_vec(&request)?;
let outcome = admission.admit(request).await?;
```

`DurableAgentAdmissionRequest` 在反序列化时会重新验证全部 Derived Field。它的
`Debug` 只暴露 Identity、Schema Reference、计数与 Digest，不暴露 Request、
Instruction、Initial State、Policy Evidence 或 Budget Payload。

精确重试必须从相同 Bytes 恢复 Request，继续使用相同 Run、Thread、Invocation、
Event 与 Checkpoint ID，并保留原不可变 Deployment Snapshot：

```rust,no_run
let retained: DurableAgentAdmissionRequest =
    serde_json::from_slice(&retry_bytes)?;

match admission.admit(retained).await? {
    AgentAdmissionCommitOutcome::Committed(stored)
    | AgentAdmissionCommitOutcome::Idempotent(stored) => {
        enqueue_or_observe(stored.run())?;
    }
}
```

PostgreSQL Provider 会在读取当前数据库时钟前探测并验证已经提交的 Evidence。因此
即使原 Deadline 已经过期，Lost ACK 也可以收敛，但只允许精确相同的 Intent、Event、
Checkpoint 与 Initial State。用相同 Run ID 更换 Input、Policy、Budget、Graph、Ready
Set 或任一 Identity 会产生 Conflict。原 Digest-pinned Executable 与 Schema 也必须
至少保留到 Retry Window 与 Recovery Window 关闭。

## Fail-closed 校验顺序

打开 Admission Transaction 前，Runtime Facade 会：

1. 解析精确 Executable Graph 闭包；
2. 要求 Agent Input/Output Schema 分别等于 Graph Input/Output Schema；
3. 使用同一冻结 Offline JSON Schema 2020-12 Registry 校验 Agent Input、
   Authorization Evidence 与 Initial State；
4. 构造并校验封闭的 Public-safe 标准 Event Data；
5. 从 Compiled Graph 推导 Initial Ready Set，而不是接受调用方提供的 Ready Set。

随后 Store 会独立加载 Tenant-scoped Immutable Graph，校验 Superstep-zero Checkpoint
与 Entry Ready Set，获取数据库时间，检查 Deadline，并写入 Transaction。Schema
拒绝、Deadline 到期、Graph Drift 与注入的晚期失败都不会留下 Run、Admission、Event、
Checkpoint 或 Scheduler Residue。

## 耐久 Evidence 与敏感数据

Migration 15 创建 `stateknot.agent_admissions`。该表保存 Canonical Admission Bytes，
以及可索引的 Agent、Graph、Policy、Digest、Event 与 Checkpoint 冗余锚点。Load 会在
同一个 Repeatable-read Snapshot 内重新验证：

- Canonical Decode 与全部 Derived Digest；
- Tenant/Run/Thread/Invocation 与 Agent Provenance；
- Graph Registry Identity、Version、Bytes 与 Definition Digest；
- Sequence-one Event Identity、Timestamp、Kind、Digest 与 Projection Digest；
- Superstep-zero Checkpoint Identity、State、Ready Set、Graph 与 Digest；
- 当前 Lifecycle、Journal/Checkpoint Head、Quarantine、Lease 与 Wait Projection。

Canonical Snapshot 可能包含用户 Input、Trusted Instruction、Principal Attribute、
Authorization Evidence 与 Policy Limit。必须把该表当成敏感应用数据：使用最小权限
Role、加密传输、加密备份、与部署相符的 Row/Tenant Isolation、访问审计和符合法规的
Retention Policy。Sequence-one Event 只保存 Operation 与关联 Digest，是 Public-safe
Metadata，但不能替代对 Snapshot 的保护。

## Migration 与 Rollout

部署调用新 Facade 的代码前，先用 Migration Role 执行 Migration 15。Rolling
Deployment 顺序为：

1. 迁移并校验 Checksum；
2. 部署理解 Admission Row 与新版 Scheduler Predicate 的 Worker；
3. 注册本 Release 的精确 Schema、Executable Graph 与 PostgreSQL Graph Definition；
4. 开启 Trusted Ingress 流量；
5. 在上一版本 Retry/Recovery Window 关闭前保留其 Digest-pinned Deployment。

禁止手工给低层 Run 添加 Checkpoint，也不能直接插入 Admission Row。发生 Conflict
时不得修改 Canonical Bytes、冗余 Column 或 Anchor Row 进行“修复”；应 Quarantine
并调查矛盾 Evidence。

## 验证证据

Core Suite 验证确定性 Budget Resolution、数据库时钟 Deadline 拒绝、Scope Coverage、
Canonical Digest 重算、Wire Tamper、Size Bound 与脱敏诊断。PostgreSQL 16/17 Suite
进一步覆盖：

- 跨越时间敏感边界后的精确 Commit 与 Retry 收敛；
- Changed-intent Conflict 与完整耐久重校验；
- 24 路同 Request Admission 只产生一次物理提交；
- 无效 Initial State Rollback 且数据库零残留；
- Migration 15 Upgrade、Index、Constraint 与 Tamper 检查；
- Runtime Facade 在任何数据库写入前拒绝 Agent/Graph 与 Authorization Schema Drift。

仓库目前会在每个支持的数据库版本上运行 106 个 PostgreSQL Provider Case 与 36 个耐久
Runtime Scenario。

## 下一条公开 Agent 边界

Admission 与 `DurableAgentRuns` 现已提供原子 Ingress Idempotency 和完整重校验的公开
Run/Result Read。Provider-native Graph、AgentService Embedding Boundary、Cancellation
Mutation 与耐久 A2A Artifact Access 已在其上实现。Stable Network Transport、类型化
API Ergonomics 与 Release Qualification 仍未完成；在此之前，StateKnot 不会声明稳定
或受生产支持的 Agent API。
