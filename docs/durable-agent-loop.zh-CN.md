<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 耐久 Agent Loop 与租户调度器

状态：已有实现与验证支撑的预发布契约。API 尚未发布且仍可能调整。本文描述仓库中已经存在的
Production-shaped 保证，不代表 StateKnot 已经达到生产发行标准。

[English](durable-agent-loop.md)

## 已交付边界

Runtime 现在闭合了从 Tenant-scoped Runnable Discovery 到耐久 Graph 生命周期边界的
Lease-owned 路径：

```text
Tenant Scheduler Tick
  -> 固定 Runnable Page Snapshot
  -> 精确 Lease Claim
  -> 耐久 Graph Driver
  -> Wait / Success / Failure Lifecycle Coordinator
  -> 一次带 Fence 的 PostgreSQL Transaction
  -> 释放 Lease 或抵达下一条耐久调度边界
```

实现刻意拆成五个职责边界：

| 组件 | 职责 |
|---|---|
| `DurableFairScheduler` | 预约一个 Replica-global 加权 Slot，给出精确 Starvation Bound，再只委托给被选中的 Tenant Worker。 |
| `DurableTenantScheduler` | 按 `(available_at, run_id)` 顺序扫描一个租户的固定 Cutoff 队列，每次最多 Claim 一个 Run，并执行一个有界 Loop Quantum。 |
| `DurableAgentLoop` | 把同一个 Store、不可变 Executable Registry、Driver 与 Lifecycle Coordinator 绑定在一起，避免不同部署快照被误配。 |
| `DurableGraphDriver` | Replay 并验证耐久 Graph 证据，先提交 Node Start 再 Dispatch，执行 Node、续租并推进 Continue Barrier。 |
| `DurableGraphLifecycle` | 使用精确 Lease-bound Handoff 原子提交 Wait、成功 Terminal 或受监督的 Run Failure。 |
| `GraphLifecycleEvidenceProvider` | 恢复由嵌入应用持久保存的 Admission、Artifact 与累计 Accounting 事实；它不是用于推测缺失数据的 Fallback Hook。 |

这是一条可运行的**耐久 Graph Loop**。Provider-neutral 耐久 Model/Tool Attempt 执行和
跨租户加权 Selection 已经存在，但它还不是稳定的最终用户 Agent API。具体 Model Adapter、
高层 Tool Ergonomics、Policy Middleware 与可编译 First-agent 教程仍属于发行前工作。

## 启动绑定

冻结 Executable Registry 前必须安装两份标准审计 Schema。数据库迁移完成后，再从同一个
不可变 Release Artifact 注册全部应用 Schema、Graph、Reducer 与 Node Executor；只有这些
步骤全部成功后才能构造 Scheduler。

```rust,ignore
use std::sync::Arc;
use stateknot_runtime::{
    DurableGraphDriverOptions, DurableGraphLifecycleOptions,
    DurableTenantScheduler, DurableTenantSchedulerOptions,
    ExecutableGraphRegistryBuilder, JsonSchemaRegistryBuilder,
    register_standard_graph_driver_event_schema,
    register_standard_graph_lifecycle_event_schema,
};

let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
register_standard_graph_driver_event_schema(&mut schemas)?;
register_standard_graph_lifecycle_event_schema(&mut schemas)?;
register_application_schemas(&mut schemas)?;

let mut executables = ExecutableGraphRegistryBuilder::new(schemas.build()?);
register_release_graphs_reducers_and_nodes(&mut executables)?;

let scheduler = DurableTenantScheduler::new(
    store,
    executables.build()?,
    Arc::new(application_lifecycle_evidence),
    DurableGraphDriverOptions::default(),
    DurableGraphLifecycleOptions::default(),
    DurableTenantSchedulerOptions::default(),
)?;
```

Lifecycle 标准审计 Schema 的不可变 Identity 为
`https://stknot.com/schemas/runtime/graph-lifecycle-event/1.0.0`。必须通过
`register_standard_graph_lifecycle_event_schema` 安装，不能把 Digest 复制到应用代码。

## 可信 Lifecycle Evidence

构造成功 `AgentResult` 需要 Graph Barrier 不拥有的事实：已 Admission 的
`AgentDescriptor`、`AgentRequest`、解析后的有限 Budget、最终 Artifact Reference 与完整累计
Usage。Terminal Failure 同样需要 Public-safe `Failure` 与累计 Usage。

嵌入服务通过 `GraphLifecycleEvidenceProvider` 提供这些事实。生产实现必须：

- 只读取可信、耐久的 Admission、Artifact 与 Accounting Store；
- 对收到的精确 Payload-free Context 保持确定性；
- 使用有界读取和 Deadline，不执行 Model、Tool 或其他外部副作用；
- 缺失数据时返回 `TemporarilyUnavailable`、`Unavailable` 或 `Corrupt`，禁止猜测 Usage、
  重建 Request 或填入零值；
- 把受保护诊断写入 Telemetry，因为公开错误会主动移除 Payload。

提交成功前，`DurableGraphLifecycle` 会用冻结的离线 Schema Registry 验证 Request Input 与
Terminal Output，构造 `AgentResult`，再验证 Provenance、Descriptor、Request、Budget、
Artifact 与累计 Usage 之间的全部关系。Evidence 失败时，Agent Loop 会有界地尝试释放精确
Fence；Run 保持可恢复，而不会被部分 Finalize。

## 原子生命周期边

### Wait

Node 代码返回 `NodeWaits`：一到六十四个完整 Interrupt 或 Timer Specification，不携带由
进程生成的 Registration Timestamp。Driver 交接时，Lifecycle Coordinator 把全部注册绑定到
同一个稳定 Lifecycle `EventId`、Tenant 与 Run，再调用
`append_worker_wait_barrier`。

一次 PostgreSQL Transaction 会同时提交 Journal Event、消费精确 Ready Result Set、写入
Successor Checkpoint、把 Run 转为 Waiting、使用数据库时间注册完整 Wait Batch，并清除
Lease。外部永远看不到部分注册的 Wait，应用服务器的时钟偏差也不会成为耐久注册证据。

### 成功 Terminal

可信证据与 Schema 校验通过后，一次 Barrier Transaction 会消费精确 Result Set、写入
Terminal Checkpoint 与 Public-safe Lifecycle Event、保存通过校验的 `AgentResult`、把 Run
转为 Succeeded，并清除 Lease。

### Blocked Failure

Same-fence In-flight 工作既不会被宣告失败，也不会被重复 Dispatch。Lifecycle Coordinator
会释放所有权，让后继 Fence 使用现有 Crash-takeover 规则。只有不存在 In-flight 工作，且至少
包含一个 Failed、Exhausted 或 Unsupported Node 的 Blocked Plan 才进入耐久监督；随后把可信
Failure Evidence、Failed Transition 与 Lease Release 放入同一 Transaction。

## Lost ACK 与 Stale Handoff

Lifecycle Handoff 是短生命周期、不可序列化的 Commit Input。Driver 在交出控制权之前只生成
一次稳定 `EventId`；每次重试都复用完全相同的 Event、Revision、Journal Head、Checkpoint
Plan 与 Fence。

对成功 Terminal 与受监督 Failure，如果第一次 Transaction 已提交但 ACK 丢失，Coordinator
只接受唯一正确的提交后快照：Revision 恰好加一、Lifecycle Status 符合预期、Journal Head
指向同一个 Event，并且 Lease 已经清除。Coordinator 会从 Lifecycle 恢复精确的已提交
`AgentResult` 或 `RunFailure`，不要求外部 Evidence Provider 再次可用。任何其他 Revision、
Event、Status、Head 或 Owner 变化都会按 Stale Terminal Handoff Fail Closed。

Wait Replay 刻意使用不同规则，因为授权 Resolver、Timer 或 Cancellation 可能在原调用方 Retry
前继续推进 Run。PostgreSQL Provider 会先查找稳定 Wait Event，再应用 Fresh-run Predicate，并
验证原始 Event Intent、Projection Digest、Checkpoint Anchor、Result Consumption 与不可变
Registration Set。因此精确 Retry 在后续 Transition 之后仍返回 Idempotent，且绝不会回滚这些
后续状态；任何不一致都会成为 Commit Conflict，不能被猜成成功。

Storage Retry 有明确上限，使用封顶指数退避，绝不分配替代 Durable Identity。Driver 或
Lifecycle 出错时会有界尝试释放精确 Fence；如果 Cleanup 自身发生数据库错误，API 会同时保留
Primary Error 与 Cleanup Error，便于运维区分执行失败和所有权清理失败。

## Tenant Scheduler 契约

一次 `DurableTenantScheduler::tick` 会：

1. 按耐久 Queue Order 扫描固定数据库时间的 Tenant Snapshot；
2. 限制每页 Decode 数量与最大 Page Chain；
3. 为每个 Candidate 分配一个稳定 UUIDv7 `AttemptId`，Transient Claim Retry 复用它；
4. 把 Lease Contention 或 Discovery 后发生变化的 Candidate 视为普通 Skip；
5. 最多 Claim 并执行一个 Run；
6. 返回闭合 Outcome，以及 Page、Candidate、Contention 与 Retry Counter。

`Executed`、`ExecutionFailed`、`Idle`、`ScanLimitReached` 与 `Cancelled` 互不混淆。单个 Run
的 Agent Loop Error 不会杀死 Tenant Worker；基础设施级 Scan/Claim Error 才会返回 Scheduler
Error。

部署通过运行明确配置数量的 Worker 获得有界并发，数据库 Fencing 负责解决竞争。
`DurableTenantScheduler` 刻意保持 Single-tenant；需要 Replica-safe Smooth Weighted Selection
与精确 Reservation-count Starvation Bound 时，通过 `DurableFairScheduler` 包装。详见
[公平调度合约](cross-tenant-fair-scheduler.zh-CN.md)。不能通过给 Tenant Worker 跨租户数据库凭据或
Queue Scope 来伪造 Fairness。

## 运维与可观测性

至少应导出：

- Scheduler Page/Candidate 扫描量、Contention Skip、Claim Retry、Scan-limit Outcome 与
  Per-tenant Queue Age；
- Driver Replay/Result Bytes、Durable Start/Completion、Continue Barrier、Renewal、Timeout、
  Cancellation 与 Mutation Retry；
- Lifecycle Wait/Success/Failure Commit、Idempotent Recovery、Stale Handoff、Evidence Error
  Class、Exact-fence Release 与 Cleanup Failure；
- Run Status、Lease Age、Delayed-retry Age 与 Waiting 时长，同时禁止记录 Request、Output、
  Failure 或 Secret Payload。

启动时使用仅有 DDL 权限的 Migration Credential，随后改用 Least-privilege Trusted Runtime
Credential。Database Statement Timeout 必须小于 Lease Safety Margin。Evidence Provider
Deadline 必须能放进保留的 Handoff Lease；超时后应释放所有权，而不是尝试 Stale Commit。

## 验证证据与剩余门禁

十六个 Runtime Integration Scenario 会分别在 PostgreSQL 16 与 17 上运行，覆盖 Lifecycle
Success/Wait/Failure 原子性与精确 Lost-ACK Replay、数据库时间 Wait Materialization、Agent
Loop 成功与 Evidence Failure、Tenant 与加权 Cross-tenant Scheduling、耐久 Model/Tool Attempt
与 Streaming、Noninitial Replay、Same-fence Suppression、Lease Renewal、Near-expiry Refresh、
初始状态 Quarantine 与 Higher-fence Takeover。每个数据库版本还会运行 95 个 Provider
Integration Case；CI 把两套测试都设为 Mandatory。

剩余 Release Blocker 包括生产 Admission/Accounting Provider、具体第一方 Model Adapter、
公开 Agent API 与可编译示例、Parallel Sibling Policy、Loop/Subgraph 语义、协议专用 Outbox
Dispatch、数据库角色隔离存储过程、通用 Retention、Backup/Restore、Failover、Stale-race
Qualification、完整 Observability 与 Release Hardening。
