<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 耐久 Agent Run 与 Result

状态：已实现的 pre-alpha 集成契约。Crate 尚未发布，API 也还没有兼容性承诺。

本文定义接纳、解析并读取一个耐久 Agent Run 的公开 Rust 边界，覆盖
`DurableAgentRuns`、Tenant-scoped Ingress Idempotency、PostgreSQL Migration
16、安全 Run/Result Snapshot 与生产集成规则。认证、授权、HTTP Routing、限流和
Ownership Check 仍由嵌入 StateKnot 的 Control Plane 负责。

英文版见 [Durable Agent runs and results](durable-agent-runs.md)。

## 构建一份不可变 Runtime

`DurableAgentRuns` 把 `PostgresStore` 与冻结的 `ExecutableGraphRegistry` 绑定。
按照[耐久 Admission 契约](durable-agent-admission.zh-CN.md)注册全部
Digest-pinned 应用 Schema 和标准 Schema，安装精确 Graph、Reducer、Node 闭包，
冻结 Registry，再为 Tenant 把同一 Compiled Graph 注册到 PostgreSQL。

```rust,no_run
let runs = DurableAgentRuns::new(store.clone(), executable_registry)?;
```

每次 Admission 与 Load 都会再次解析不可变 Graph，并重新校验 Agent/Graph 的
Input/Output Schema 绑定、Request Input、Authorization Evidence、Initial State、
Admission Event 与 Terminal Output。缺少精确 Executable 或 Schema 的 Deployment
会 Fail Closed，不会返回未经验证的结果。

## 使用耐久 Idempotency 提交

面向用户的 Ingress 应使用 `submit`。`admit` 仍供能够在结果不明时保留完整 Request
和全部生成 Identity 的内部调用方使用。

```rust,no_run
let key = AgentSubmissionKey::generate();

let request = DurableAgentAdmissionRequest::new(
    tenant_id.clone(),
    AgentRunIds::generate(),
    agent_descriptor,
    agent_request,
    evaluated_budget_layers,
    graph_reference,
    admission_authority,
    initial_state,
)?;

let submitted = runs.submit(&key, request).await?;
let snapshot = submitted.snapshot();
return_key_and_run_id_to_caller(&key, snapshot.provenance().run_id())?;
```

首次请求前，调用方必须提供或保存 Key。默认生成的 Key 由两个独立 UUIDv7 组成，
合计包含 148 位随机量。外部 Key 必须由 `[A-Za-z0-9._~-]` 中的 16–128 个字节
组成；Client 仍应使用至少 128 位密码学不可预测性。Raw Key 属于敏感关联数据，但
绝不能当作认证凭据。

Provider 只保存 Raw Key 的 Tenant-scoped SHA-256 Digest。Mapping 与新 Admission
在同一个 Transaction 提交。Mapping 绑定不可变 Agent、Request、排序后的 Budget
Layer 与 Resolved Budget、Graph、Authorization Snapshot、Initial State 和 Initial
Ready Set；Framework 生成的 Run、Thread、Invocation、Event 与 Checkpoint Identity
刻意不进入 Logical Submission Digest。

发生 Lost Response 后，重建相同 Logical Content 并复用同一 Key。可以生成新的
`AgentRunIds`，Provider 仍会返回第一次选中的 Run：

```rust,no_run
let retry = DurableAgentAdmissionRequest::new(
    tenant_id.clone(),
    AgentRunIds::generate(), // Retry 可生成新的 Candidate ID Bundle
    retained_agent_descriptor,
    retained_agent_request,
    retained_budget_layers,
    retained_graph_reference,
    retained_admission_authority,
    retained_initial_state,
)?;

match runs.submit(&key, retry).await? {
    AgentRunAdmissionOutcome::Committed(snapshot)
    | AgentRunAdmissionOutcome::Idempotent(snapshot) => observe(snapshot)?,
}
```

同一 Key 携带不同 Logical Content 会返回
`StoreError::AgentSubmissionConflict`。一个耐久 Run 最多拥有一个 Submission Key；
为同一保留 Admission 换另一个 Key 也会 Conflict。不同 Tenant 中相同 Raw Key 相互
独立。Raw Key 不会持久化，也不会出现在 `Debug` 输出中。

Retry 时不能重新生成 Policy Evidence、Deadline、Budget Layer、Agent Definition 或
Initial State；至少在完整 Client Retry Window 内保留这些不可变输入。完成确定性的本地
Deployment 与 Request 重校验后，Provider 会先检查已有 Key Evidence，再执行新的数据库
时钟与 Initial-checkpoint Admission 检查。因此 Lost ACK 即使跨过原 Deadline 也只会
解析到原 Run；缺失精确 Executable 的 Deployment 则仍然 Fail Closed。

## 按 Run 或 Key 读取

调用任一方法前，先认证并授权 Tenant 与目标资源：

```rust,no_run
authorize_run_read(&principal, &tenant_id, requested_run_id)?;
let by_run = runs.load(&tenant_id, requested_run_id).await?;

authorize_submission_read(&principal, &tenant_id)?;
let by_key = runs.load_by_key(&tenant_id, &key).await?;
```

`TenantId`、`RunId` 与 `AgentSubmissionKey` 都不是访问证明。禁止向不可信 Client
直接暴露 Store 或数据库 Role。Not-found 与 Conflict 应经过应用拥有的错误策略转换，
不得泄漏 Cross-tenant 资源是否存在。

两个读取路径都会在同一 Repeatable-read Database Snapshot 中加载 Admission、Graph、
初始 Event/Checkpoint、当前 Lifecycle、Wait Projection，以及适用时的 Submission
Mapping；返回公开数据前会重新推导 Canonical Bytes 和全部冗余 Digest。

## 公开 Snapshot 合约

`AgentRunSnapshot` 刻意排除 Request Input、Authorization Evidence、Graph State、
Lease、Scheduler Internal 与 Private Diagnostic，只包含：

- 可信 Agent Result Provenance，包括 Tenant、Run、Thread、Invocation 与精确 Agent
  Identity；
- 不可变 Graph Reference 与 Admission Digest；
- 数据库时钟的 Admission 与最新 Lifecycle Observation；
- 单调 Lifecycle Revision 与协议无关 Status；
- Quarantine 标记；
- 必须存在、但允许为 `null` 的 `outcome` 字段。

Polling 或 Cache 应使用 `(run_id, revision)` 作为 Observation Key；Revision 不变表示
Lifecycle State 不变。`active`、`waiting` 与 `cancellation_requested` 的 `outcome` 为
`null`；`succeeded`、`failed` 或 `cancelled` 时必须存在对应对象。省略该字段属于无效
Wire Shape，从而能区分旧版响应和已知尚未终止的 Run。

成功结果包含完整 Rebind 的 `AgentResult`。每次读取都会重新校验 Provenance、Request、
Descriptor、Output Schema、累计 Usage、有限 Budget 与 Digest-pinned Output Schema。
只有 Facade 返回结果后，应用才能把 `result.output()` 解码为生成的 Rust Output Type。

Failed 与 Cancelled Outcome 只暴露 Public-safe `Failure`、耐久 Completion Time 与累计
Usage。Cancellation Failure 必须使用 `Cancelled` Category 和 `Never` Retry Advice；
普通 Failure 不能使用 Cancellation Category。Failed/Cancelled Usage 不要求仍位于
Budget 内——触发终止的原因本身可能就是 Budget 或 Deadline 耗尽。

Quarantined Run 仍可读取，且 `is_quarantined() == true`，但不得被表示为可执行工作；
公开读取边界不会静默清除 Quarantine。

## Migration 16 与 Rollout

Migration 16 创建 `stateknot.agent_submission_keys`，增加精确 Admission Reference
Key，并安装 Tenant Grammar、UUIDv7 Run Identity、32-byte Digest、One-key-per-run 与
指向选定 Admission 的 Composite Foreign Key 约束。数据库只保存 Tenant-scoped Key
Digest。Created-time Index 支持运维清点，不会削弱 Idempotency Invariant。

按以下顺序发布：

1. 先备份并验证 Restore 流程；
2. 使用 Migration Role 执行 Migration 15 与 16；
3. 只有 `PostgresStore::verify_schema` 接受全部 Version、Checksum、Table、Index 与
   Constraint 后才能启动 Binary；
4. 部署冻结的 Executable/Schema Registry，并注册 Tenant Graph；
5. 开启 `submit` 流量；
6. 在完整 Retry 与 Recovery 生命周期内保留旧 Executable Snapshot、Admission Row
   与 Key Mapping。

只要 Client 仍可能 Retry，就绝不能删除 Key Mapping：删除会把安全 Retry 变成创建
第二个 Run 的许可。StateKnot 当前不提供独立 Key 删除 API。未来的 Run Retention
实现必须在全部 Retry Guarantee 到期后，以一个显式有界操作同时移除 Run、Admission
与 Key Evidence。

## 验证证据

PostgreSQL 16/17 Matrix 覆盖：带既有数据的 Migration 15→16 Upgrade、同 Key 配合
全新 ID Bundle 的 Retry、Changed-content 与 Second-key Conflict、24 个并发 Candidate
收敛为一次物理 Commit、Raw Key 不落库、Mapping Tamper Detection，以及注入 Mapping
失败后 Run、Event、Checkpoint、Admission、Mapping 全部 Rollback。Runtime Suite
覆盖 Active 与 Cancellation-requested Snapshot，并分别暴露验证后的 Succeeded、
Failed 与已确认 Cancelled Outcome；测试会按 Run 与 Key 重新加载，重校验 Output 与
Provenance，并拒绝不可能、字段不完整或 Failure Kind 错配的 Public Wire Snapshot。

当前仓库在每个受支持数据库版本包含 102 个 PostgreSQL Provider Scenario 与 27 个
耐久 Runtime PostgreSQL Scenario。这些是实现事实，不代表生产支持声明；Release
Qualification、HTTP/SSE Service Role、已发布 Crate、通用 Retention 与兼容性保证仍在
v1 之前完成。
