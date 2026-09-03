<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 耐久 Graph Runtime

状态：已有实现与验证支撑的预发布契约。API 尚未发布，仍可能调整；本文只描述当前真正
实现并通过验证的边界，不代表已经达到生产发行标准。

[English](durable-graph-runtime.md)

## Runtime 负责什么

`stateknot-runtime` 把 Canonical Compiled Graph 与本地可执行代码精确绑定。Run 执行期间
不会从网络发现 Schema 或代码。每份不可变部署快照包含：

- `$id`、RFC 8785 Canonical Bytes、Version 与 SHA-256 Digest 均和
  `SchemaReference` 精确一致的 JSON Schema 2020-12 文档；
- 每个 Reducer Revision 唯一对应的纯 `GraphReducer`；
- 每份完整 Graph Digest 中每个 Node 唯一对应的 `GraphNodeExecutor`；
- 不存在任何注册 Graph 永远无法使用的孤立 Reducer 或 Node 实现。

注册表构建是启动门禁。Schema、Reducer 或 Node 缺失，Identity 被不同内容复用，`$ref`
无法离线解析，重复绑定或孤立代码都会让进程在领取任务前启动失败。`build` 之后注册表不可变，
运行时也没有在线拉取 Schema 的路径。

对于一个精确且仍存活的 `RunFence`，`DurableGraphDriver` 会：

1. 重新加载并编译 Checkpoint 固定的 Graph Definition；
2. 使用精确的本地 Reducer 与 Schema，在有界内存中独立重放所有已提交的非初始
   Checkpoint；
3. 生成确定性的 Ready-node Recovery Plan；
4. 先持久化 Physical Node Attempt Start，再启动 Node 代码；
5. 只有新返回 `NodeAttemptCommitOutcome::Committed` 才获得执行授权；
6. Node 启动前刷新临近过期的 Lease；执行期间不持有数据库事务，并以数据库时间换算的
   Monotonic Watchdog 负责续租、单调取消和硬超时；
7. 按最新 Journal Head 提交 Success 或 Public-safe Failure；
8. 自动提交 Continue Barrier 并继续执行；
9. 当下一条生命周期边不属于 Driver 时，返回类型化 Handoff。

`Idempotent` Start 绝不是执行授权：它既可能表示上次 ACK 丢失，也可能表示另一个执行器已经
成功写入 Start，因此必须按 In-flight 处理。如果原执行器死亡，只能等 Lease 到期或显式
Supersede 后，由更高 Fence 恢复这次未完成执行。

## 启动集成

数据库迁移与可执行注册都属于部署启动流程，不能放到请求热路径。使用拥有 DDL 权限的凭据
执行 `PostgresStore::migrate_database` 后立即丢弃，再用最小权限 Runtime 凭据调用
`PostgresStore::connect`。

启动顺序必须是：

1. 创建 `JsonSchemaRegistryBuilder`；
2. 调用 `register_standard_graph_driver_event_schema`；
3. 调用 `register_standard_graph_lifecycle_event_schema`；
4. 调用 `register_standard_agent_cancellation_event_schema`；
5. 注册应用全部 Digest-pinned Schema，并 `build` 冻结；
6. 把每个 `CompiledGraph`、精确 `GraphReducer` 以及该 Graph 的全部
   `GraphNodeExecutor` 加入 `ExecutableGraphRegistryBuilder`；
7. `build` 验证闭包；
8. 用同一 `PostgresStore`、不可变注册表与显式 Options 构造 Driver、Lifecycle 或 Agent Loop；
9. 只有上述步骤全部成功后才让 Scheduler 开始 Claim。

Driver 标准审计 Schema 的不可变标识为
`https://stknot.com/schemas/runtime/graph-driver-event/1.0.0`。应用应调用注册函数取得
Digest，不应把 Digest 手工复制到业务代码。

Lifecycle Coordinator 使用独立的不可变 Schema
`https://stknot.com/schemas/runtime/graph-lifecycle-event/1.0.0`。构造
`DurableGraphLifecycle`、`DurableAgentLoop` 或 `DurableTenantScheduler` 的部署必须在冻结注册表
前通过 `register_standard_graph_lifecycle_event_schema` 安装它。

Cancellation Confirmation 使用第三份不可变 Schema：
`https://stknot.com/schemas/runtime/agent-cancellation-event/1.0.0`。同一部署必须通过
`register_standard_agent_cancellation_event_schema` 安装它；缺失时 Lifecycle 构造会
Fail Closed。

Graph、Reducer、Node Executor 必须来自同一 Release Artifact。Run Admission 使用的完整
Identity、Version 与 Digest 必须和执行器注册值一致；不能在相同 Version 下偷偷替换实现。

## Claim 与执行

Scheduler 先读取 Runnable Projection，选择 Run，分配稳定的 UUIDv7 `AttemptId`，然后调用
`PostgresStore::claim_lease`。只有成功 Claim 或相同 Attempt ID 的 Idempotent Claim 才能
取得传给 `drive` 的精确 `RunFence`；其他 Owner 导致的 `LeaseHeld` 是普通竞争。

传入的 Shutdown Signal 必须由进程拥有并保持单调。Node 执行期间若 Shutdown 生效，Driver
会先发出协作取消，Node 不返回时再 Abort Task，随后释放精确 Fence，并保留已经持久化但未
完成的 Attempt，交给更高 Fence 恢复。

进程 Shutdown Signal 与耐久 Run Cancellation 是两条不同边界。Node Active 期间，Driver
会轮询 Run Projection；观察到 `cancellation_requested` 后停止新 Dispatch、向 Node 发出
Cancellation、只等待配置的 Cooperative Grace Period，再返回精确 Lease-bound
Cancellation Handoff 供 Lifecycle 确认。

Model 与 Tool 外部副作用必须通过各自的 Durable Invocation Ledger。Node 中直接执行的裸
外部写入无法继承 StateKnot 的幂等与 Reconcile-first 保证。

## Outcome 处理契约

每种 `GraphDriveOutcome` 都有唯一明确的责任方：

| Outcome | 调用方必须做什么 | Lease 状态 |
|---|---|---|
| `LifecycleBarrierReady` | 立即构造完整 Wait 或成功 Terminal 元数据，使用 Handoff 中原样的 Plan、Journal Head、Revision 与 Lease 调用对应的原子 Provider Commit。禁止重建或拆分保存。 | Driver 保留；提交时必须仍有效 |
| `Blocked` 且 `in_flight > 0` | 监督或放弃当前所有权；禁止在同一 Fence 下再次 Dispatch。 | 保留 |
| `Blocked` 且包含 `failed` / `exhausted` | 使用 Recovery Plan 的精确证据与累计 Usage 执行 Run-level Failure Policy，禁止自行推导 Retry 权限。 | 保留 |
| `Deferred` | 不要在进程内再注册 Timer；数据库时间的索引 Gate 已提交。 | Driver 已释放 |
| `Yielded` | 若仍有任务，通过 Scheduler Discovery 和新 Claim 再次进入。 | Driver 已释放 |
| `CancellationRequested` | 立即使用原样 Handoff 调用 Lifecycle Cancellation Confirmation；普通应用应使用会自动完成该步骤的 `DurableAgentLoop`。禁止合成 Usage。 | Fresh Confirmation Commit 前由 Driver 保留且必须仍有效 |
| `Cancelled` | 进程 Shutdown 已停止本地工作；后续 Scheduler Claim 负责恢复。 | Driver 已释放 |

`GraphLifecycleBarrierHandoff` 刻意不能序列化，也不能脱离 `RunLease` 作为队列消息。它是
短生命周期的原子提交输入。如果生命周期服务不能在 Lease 过期前完成，就应释放或等待过期，
然后让新 Fence 重新规划，绝不能拿过期 Handoff 提交元数据。

普通应用 Worker 应优先使用 `DurableAgentLoop`，而不是自行处理这些 Outcome。它会把 Driver 与
`DurableGraphLifecycle` 绑定到同一个 Store 和 Registry，提交 Lifecycle Handoff，并在错误后
有界清理精确 Fence。只有实现同等所有权规则的专用编排服务才应直接集成 Driver。详见
[Agent Loop 与 Tenant Scheduler 契约](durable-agent-loop.zh-CN.md)。

## 资源与时间配置

`DurableGraphDriverOptions` 必须依据真实负载边界配置：

- `GraphReplayLimits` 限制每个历史 Barrier 可以保留的 Compact Pending Result 总字节；
  默认 64 MiB，硬上限 512 MiB。
- `maximum_durable_events` 限制一次 `drive` 的工作量；只会在 Durable Operation 之间
  Yield，默认 1,024。
- `lease_renewal_interval` 至少要在 Provider Lease Duration 内容纳三次，并且不能超过
  Maximum Renewal Horizon。
- `node_execution_timeout` 是 Node 的硬 Wall-clock Deadline；默认 15 分钟，硬上限 7 天。
- `cancellation_poll_interval` 与 `cancellation_grace_period` 默认分别为 250 ms 与 5 s，
  Polling 被限制在 10 ms–60 s，Grace 的硬上限为 5 分钟；必须依据 Provider/Tool 实测取消
  行为和 Lease Safety Margin 设置。
- Mutation Retry 必须复用同一 Event/Attempt Identity，以有界次数和封顶指数退避执行。

Durable Start 提交后，Driver 会先取得新的数据库时间 Lease Observation，确认安全余量后才
启动 Node。每次 Renewal 都与一个在数据库请求前锚定的保守 Monotonic Deadline 竞速。
迟到的 `Idempotent` 响应只能证明 Renewal 已经提交，不能让已过期 Lease 重新获得授权；
Watchdog 会先取消 Node。

数据库 Statement Timeout 必须小于 Lease Safety Margin；Provider、Tool 与 Model 请求超时
必须小于 Node Deadline。生产监控应采集 `GraphDriveReport` 的 Replay Checkpoint/Barrier/
Result 数量与字节、Start、Completion、Barrier、Renewal 和 Mutation Retry 计数。

## 恢复与安全不变量

- Node Start 一定先于代码执行持久化。
- `Idempotent` Start 永远不执行代码。
- Completion 是 Append-only，并且精确引用对应 Physical Start。
- Stale Fence 不能续租、完成、调度、释放或提交 Barrier。
- Noninitial Replay 必须使用原始提交时相同的 Graph、Reducer、Schema、Journal Anchor、
  Result Set 与 Checkpoint Parentage。
- Durable Graph 证据缺失或矛盾时，当前 Live Fence 必须先把 Run Quarantine，再允许返回。
- Driver 只会自主提交 Continue；Wait 与 Terminal 必须经过生命周期集成层。
- 当前同一 Run 的 Ready Sibling 按稳定 Plan 顺序串行执行，以保持唯一 Journal Predecessor
  和恢复授权。并行 Sibling 需要独立的有界排序与 Admission Policy，通过验证前不会打开。

## 已验证证据与剩余门禁

二十九个 Runtime 场景会在 PostgreSQL 16 与 17 独立运行，其中六个保留 Driver 专属恢复覆盖：

1. Continue Barrier 提交后执行 Noninitial Replay，并交出 Terminal Handoff；
2. Same-fence In-flight 恢复不重复调用 Executor；
3. Node 执行超过原始 Lease 后仍通过续租完成；
4. 临近过期的 Claim 会在 Node 代码启动前刷新；
5. 非法初始 Checkpoint State 会在任何 Executor 调用前被 Quarantine；
6. 更高 Fence 对未完成 Physical Attempt 只接管一次。

Lifecycle/Scheduler 覆盖会验证 Success Terminal、Wait、受监督 Failure 与 Cancellation
Handoff 的原子提交和精确 Lost-ACK Retry、数据库时间 Wait Registration、Agent Loop 成功与
Evidence-unavailable Cleanup，以及 Tenant Scheduler 的选择、Claim、执行和 Idle 收敛。
Provider-native 场景还验证多轮 No-redispatch Recovery、Stale Policy Race、已知失败 Tool
Continuation、Exact-usage Cancellation、Pre-dispatch Cancellation 与 Evidence Unavailable
时的 Fail-closed 行为。PostgreSQL Provider Suite 会在每个数据库版本上独立运行。CI 把两套
外部数据库测试都设为 Mandatory；数据库服务缺失时必须失败，不能静默跳过。

后续 Typed Agent 里程碑已经提供第一批 OpenAI Responses 与 Anthropic Messages Adapter；
原子 Admission、公开耐久 Run/Result Facade，以及带 Policy、精确 Accounting、Transcript
Recovery 与 Cancellation Confirmation 的预置 Provider-native Graph 也已经实现。仍未完成的
包括 Public Cancellation Transport、Parallel Sibling、
Loop/Subgraph、协议专用 Outbox Adapter、数据库角色隔离存储过程、归档保留、
Failover/Restore 验证、10,000 次 Stale-race 门禁或稳定公共发行。这些都是明确 Release
Blocker，不会用隐藏的降级逻辑替代。
