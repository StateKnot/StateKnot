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
3. 注册应用全部 Digest-pinned Schema，并 `build` 冻结；
4. 把每个 `CompiledGraph`、精确 `GraphReducer` 以及该 Graph 的全部
   `GraphNodeExecutor` 加入 `ExecutableGraphRegistryBuilder`；
5. `build` 验证闭包；
6. 用同一 `PostgresStore`、不可变注册表与显式 `DurableGraphDriverOptions` 构造 Driver；
7. 只有上述步骤全部成功后才让 Scheduler 开始 Claim。

Driver 标准审计 Schema 的不可变标识为
`https://stknot.com/schemas/runtime/graph-driver-event/1.0.0`。应用应调用注册函数取得
Digest，不应把 Digest 手工复制到业务代码。

Graph、Reducer、Node Executor 必须来自同一 Release Artifact。Run Admission 使用的完整
Identity、Version 与 Digest 必须和执行器注册值一致；不能在相同 Version 下偷偷替换实现。

## Claim 与执行

Scheduler 先读取 Runnable Projection，选择 Run，分配稳定的 UUIDv7 `AttemptId`，然后调用
`PostgresStore::claim_lease`。只有成功 Claim 或相同 Attempt ID 的 Idempotent Claim 才能
取得传给 `drive` 的精确 `RunFence`；其他 Owner 导致的 `LeaseHeld` 是普通竞争。

传入的 Shutdown Signal 必须由进程拥有并保持单调。Node 执行期间若 Shutdown 生效，Driver
会先发出协作取消，Node 不返回时再 Abort Task，随后释放精确 Fence，并保留已经持久化但未
完成的 Attempt，交给更高 Fence 恢复。

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
| `Cancelled` | 停止本地工作；后续 Scheduler Claim 负责恢复。 | Driver 已释放 |

`GraphLifecycleBarrierHandoff` 刻意不能序列化，也不能脱离 `RunLease` 作为队列消息。它是
短生命周期的原子提交输入。如果生命周期服务不能在 Lease 过期前完成，就应释放或等待过期，
然后让新 Fence 重新规划，绝不能拿过期 Handoff 提交元数据。

## 资源与时间配置

`DurableGraphDriverOptions` 必须依据真实负载边界配置：

- `GraphReplayLimits` 限制每个历史 Barrier 可以保留的 Compact Pending Result 总字节；
  默认 64 MiB，硬上限 512 MiB。
- `maximum_durable_events` 限制一次 `drive` 的工作量；只会在 Durable Operation 之间
  Yield，默认 1,024。
- `lease_renewal_interval` 至少要在 Provider Lease Duration 内容纳三次，并且不能超过
  Maximum Renewal Horizon。
- `node_execution_timeout` 是 Node 的硬 Wall-clock Deadline；默认 15 分钟，硬上限 7 天。
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

以下六个 Driver 场景都在 PostgreSQL 16 与 17 独立运行：

1. Continue Barrier 提交后执行 Noninitial Replay，并交出 Terminal Handoff；
2. Same-fence In-flight 恢复不重复调用 Executor；
3. Node 执行超过原始 Lease 后仍通过续租完成；
4. 临近过期的 Claim 会在 Node 代码启动前刷新；
5. 非法初始 Checkpoint State 会在任何 Executor 调用前被 Quarantine；
6. 更高 Fence 对未完成 Physical Attempt 只接管一次。

此外，每个数据库版本还运行 91 个 Provider Integration Test。CI 把外部数据库套件设为
Mandatory；数据库服务缺失时必须失败，不能静默跳过。

本阶段尚未提供跨租户 Scheduler Fairness、完整 Wait/Terminal/Failure Handoff 生命周期
服务、Model/Tool Agent Loop、协议专用 Outbox Adapter、数据库角色隔离存储过程、归档保留、
Failover/Restore 验证、10,000 次 Stale-race 门禁或稳定公共发行。这些都是明确 Release
Blocker，不会用隐藏的降级逻辑替代。
