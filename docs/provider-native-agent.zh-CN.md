<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Provider-native Agent Graph

本文是未发布 `stateknot-runtime` 中 `ProviderNativeAgentGraph` 的生产集成契约。
该实现把强类型 Agent Descriptor、耐久 Invocation Ledger、可执行 Graph Registry、
Graph Driver、Lifecycle Coordinator 与 Agent Loop 组合成一套有界 Model/Tool 状态机。
它仍处于 pre-alpha：API 尚不稳定、Crate 尚未发布，仓库也尚未声明生产支持。

最短且诚实的入口是已经编译验证的 No-I/O 示例：

```console
cargo run -p stateknot-runtime --example provider_native_agent --locked
```

它会真实构造支持 Tool 的 Descriptor、Digest-pinned 本地 Policy 与 Accounting
合约、生成的 Checkpoint Schema、全部必需的标准 Runtime Schema 和初始状态；它明确
不执行 Provider 或数据库 I/O。耐久执行由 PostgreSQL 16/17 真库测试单独验证。

## 已实现的执行子集

当前 v1 Graph 只接受以下执行语义：

- Model-native JSON Schema Output；
- 不超过 Descriptor 中有限的 `max_model_turns`；
- 每轮不超过有限的 `max_tool_calls_per_turn`；
- 按 Provider Proposal 顺序串行执行 Tool；
- Output Repair Turn 必须为零；
- 每个 Checkpoint 最多保留 4,096 个紧凑 Model/Tool Invocation 引用。

`ProviderNativeAgentGraph::compile` 会拒绝以 Tool Call 模拟最终输出、Repair Turn、
并行 Tool Dispatch、过大的调用上限，以及无法装入耐久 Superstep 范围的组合。它不会
静默降级为更弱的行为。

生成的 Graph 有两个稳定的可执行 Node：

1. `agent.model` 从不可变 Invocation Ledger 重建完整 Provider-native Transcript，
   Prepare 或恢复一个 Model Attempt，并产生最终 Model-native Output 或提交 Tool Route。
2. `agent.tools` 依据已 Admission 的 Descriptor 解析每个 Proposal、校验 Arguments、
   执行固定版本 Policy，再按顺序执行或恢复 Tool，最后返回 `agent.model`。

Checkpoint 只保存组合 Digest、稳定 Input Message ID、有界 Invocation 引用与下一 Phase。
Provider Response、Tool Result 与累计 Usage 始终保存在各自的不可变 Ledger 中。

## 冻结一个完整 Deployment Snapshot

Policy 与 Accounting Reference 都属于 Composition Digest。修改 Policy 代码、Price
Table、Agent/Model/Tool Descriptor、Schema Profile、Instruction、Execution Limit 或
Input Security Label，都必须发布新的 Graph Version。

```rust,ignore
let definition = ProviderNativeAgentGraph::compile(
    typed_definition.descriptor().clone(),
    graph_identity,
    reducer_identity,
    state_schema_id,
    input_security_label,
    policy,
    accounting,
)?;

definition.register_schema(&mut schema_builder)?;
register_standard_graph_driver_event_schema(&mut schema_builder)?;
register_standard_graph_lifecycle_event_schema(&mut schema_builder)?;
register_standard_agent_cancellation_event_schema(&mut schema_builder)?;
register_standard_agent_admission_event_schema(&mut schema_builder)?;
register_standard_invocation_execution_event_schema(&mut schema_builder)?;
let schemas = schema_builder.build()?;
```

冻结 Registry 前，还必须注册 Typed Agent Input/Output Schema、所有 Tool Input/Output
Schema 和全部 Provider Compatibility Profile。随后 `register_executable` 把同一个
`PostgresStore`、`DurableInvocationExecutor` 与不可变 Schema Snapshot 绑定起来。任何
Digest-pinned 依赖缺失或冲突都会让启动失败。

不要修改正在运行的 Registry。应构建完整的新 Deployment Snapshot，耐久注册其
Compiled Graph，再让新 Run Admission 到这个精确 Graph Reference；存量 Run 继续解析
自己已经固定的版本。

## Policy 与 Accounting 是执行依赖

`AgentToolPolicy` 在 Tool Prepare 前执行。它必须无副作用、在本地运行，并对精确
Context 保持确定性；返回值包含不可变 Decision Evidence 的 Digest。Network Policy
Engine 需要自己的耐久 Decision Ledger，不能隐藏在这个同步边界后面。

Action Digest 绑定已 Admission Agent、Admission Digest、已提交 Model Invocation、
Proposal Position、Tool Identity 与 Canonical Arguments。获准的 Tool Plan 会同时保留
Action Digest 与 Policy-evidence Digest；Recovery 会在任何 I/O 前重新校验两者。

`AgentInvocationAccounting` 只对已经耐久化的 Terminal Ledger Evidence 计价，必须离线
且确定性。只有真正免费的 Invocation 才能返回 `Known(KnownCosts::empty())`。无法获得
精确价格时返回 `Unpriced`；StateKnot 会保留 Usage Evidence，并在有限 Monetary Budget
无法继续计算时阻止下一次调用。缺失价格绝不会被转换为零成本。

## 耐久 Dispatch 与 Recovery

每次外部 Attempt 都遵循同一条权限顺序：

1. 校验 Checkpoint、Transcript、Descriptor、Schema、Policy、Budget、Deadline 与
   Lease/Fence；
2. Prepare Logical Invocation 及其稳定 Event Identity；
3. 在外部 Dispatch 前提交 Physical Attempt Start；
4. 只有 Start 新返回 `Committed` 时才允许 Dispatch；
5. Append Terminal Provider/Tool Evidence；
6. 依据精确 Ledger Revision 提交 Node Result 与下一 Checkpoint。

幂等观察到的 Start 不授予 Dispatch 权限。Lost ACK 或进程崩溃后，Recovery 读取已提交
Terminal Ledger，而不是再次调用 Provider。更高 Fence 可以恢复未完成的 Physical Node
Attempt，但不能改写已经提交的外部结果。

已知失败的 Tool 会继续作为有序 Transcript Outcome；其精确 Terminal Revision 绑定到
Node Result，下一轮 Model 会收到失败结果，而不是虚构 Success。Write Tool 的 `Unknown`
Outcome 必须等待 Status Query、Idempotency Proof、Compensation 或人工 Reconciliation；
Graph 不会把它当作普通失败自动 Retry。

## 两阶段耐久 Cancellation

Cancellation Intent 与 Cancellation Completion 是两条不同事实。

1. 已认证 Control-plane Service 使用不可变 Cancellation Failure 与 Audit Event 提交
   `RunTransition::RequestCancellation`。仓库当前尚未提供稳定公开 HTTP Cancellation
   Endpoint；授权与 Request Schema 属于嵌入服务边界。
2. Node Active 时，Driver 轮询耐久 Run State，发出 Cooperative Cancellation，在配置的
   Grace Period 内等待，必要时 Abort 本地任务。观察到 Request 后不再 Dispatch 新
   Activation。
3. Driver 返回精确 `GraphCancellationHandoff`，其中包含 Checkpoint、Journal Head、
   Revision、Failure ID、Event ID 与 Live Lease。
4. `DurableAgentLoop` 把 Handoff 交给 `DurableGraphLifecycle`；后者从可信 Ledger 重建
   Cumulative Usage，并把 `agent_cancellation_confirmed` 与 `ConfirmCancellation`
   原子提交。

Confirmation Timestamp 来自 PostgreSQL 时钟，Terminal Commit 同时释放 Lease。Lost ACK
即使发生在 Lease 已释放之后也可以精确重试：Coordinator 会重建已经提交的 Timestamp
与 Usage，并为同一 Event 返回 `Idempotent`。

如果 Model 仍处于 `Executing`、Write Tool 为 `Unknown`，或失败 Model 缺少精确 Usage，
`ProviderNativeAgentLifecycleEvidence` 会返回 Unavailable。Run 保持
`cancellation_requested`，不会用伪造的零 Usage 进入 Cancelled；完成 Evidence
Reconciliation 后可由 Scheduler 再次处理。

公开 Confirmation Event Schema 位于
`https://stknot.com/schemas/runtime/agent-cancellation-event/1.0.0`。它只包含可公开的关联
字段；Accounting 与 Provider Payload 保留在可信存储中。

## 运维设置

`DurableGraphDriverOptions::with_cancellation_timing` 控制耐久 Poll Interval 与
Cooperative Grace。默认值分别是 250 ms 与 5 s；Polling 被限制在 10 ms–60 s，Grace 的
硬上限为 5 min。应根据 Provider/Tool 实测行为设置，并让外部 Timeout 小于 Node
Deadline，同时为 Terminal Lifecycle Transaction 保留足够 Lease Margin。

启动时 Driver 会观察 Live Lease；当剩余时间低于配置 Lease Duration 的一半时，会在
Recovery 前先 Renew。Node 执行期间，以数据库时间观察锚定 Monotonic Watchdog。
Renewal、Cancellation Polling 与 Mutation Retry 都有界并进入 Report。

至少监控：

- Model/Tool Start、Terminal State、Recovered Terminal 与 Unknown Age；
- Checkpoint Replay 次数与保留字节数；
- Lease Age、Renewal、Stale-fence Failure 与 Takeover 次数；
- Cancellation-requested Age、Cooperative Abort、Evidence-unavailable Confirmation 与
  Idempotent Confirmation Retry；
- Token、Byte、Event、Invocation，以及 Known/Unpriced Monetary Usage；
- 按不可变 Policy Version 聚合的 Deny/Error Rate，且不记录 Arguments 或 Credential。

## 验证证据

Runtime Integration Suite 在真实 PostgreSQL 16/17 上运行 Provider-native 路径，重点
场景包括：

- Model → Tool → Model 多轮路径在 Model 已提交后恢复，不重复 Dispatch，并完成
  Lifecycle Success；
- 更高 Fence 赢得 Stale Policy Race，且无重复 External Dispatch；
- 已知失败 Tool 以正确顺序进入 Transcript，并绑定精确 Terminal Revision；
- 已提交 Model 后 Cancellation，恢复精确 Usage，并验证 Lost-ACK Replay；
- Provider Dispatch 前 Cancellation 由 `DurableAgentLoop` 自动确认；
- 精确 Evidence 不可用时，Cancellation 保持 Fail-closed。

运行 Offline 与数据库证据：

```console
cargo run -p stateknot-runtime --example provider_native_agent --locked
cargo test -p stateknot-runtime --test postgres provider_native --locked
```

第二条命令需要 `STATEKNOT_TEST_DATABASE_URL`。CI 还会设置
`STATEKNOT_REQUIRE_POSTGRES_TESTS=1`，因此基础设施缺失会直接失败，而不会静默跳过。

## 明确剩余门禁

本里程碑尚未交付 Parallel Sibling/Tool Execution、Output Repair、Loop/Subgraph 语义、
Artifact Retrieval、稳定 Network Agent/Cancellation Transport、Protocol-specific Outbox
Dispatch、MCP/A2A Server Composition、更广 Protocol Extension、A2A Live-peer
Qualification/Reconciliation、Live-provider Drift Cassette、数据库 Role Separation、通用
Retention、Failover/Restore Qualification 或生产 Release。
[`AgentServiceV1`](agent-service.zh-CN.md) 现在提供嵌入式 Service Boundary，
[`McpRemoteTool`](mcp-remote-tool.zh-CN.md) 提供严格 Client-side Tool Profile，
[`A2aRemoteAgent`](a2a-client.zh-CN.md) 提供耐久 Outbound Agent Tool Profile。独立
[MCP Server Profile](mcp-server.zh-CN.md) 与 [A2A Server Profile](a2a-server.zh-CN.md)
暴露自身 Application Boundary；它们都不扩大 Provider-native Graph 声明。这些能力
仍需要独立版本化契约与可执行证据，不能从 Provider-native Graph 推断得出。
