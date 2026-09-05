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
- 支持串行 Tool，或仅对 Descriptor 声明为 Read-only 的 Tool 进行有界并行；所有 Write
  始终串行；
- 有限的 `max_output_repair_turns`，且必须严格小于 `max_model_turns`；
- 每个 Checkpoint 最多保留 4,096 个紧凑 Tool Invocation 引用，Model Invocation
  引用数量受有限的模型轮次上限约束。

`ProviderNativeAgentGraph::compile` 会拒绝以 Tool Call 模拟最终输出、过大的调用上限、
非法并发边界、与保留 Repair Instruction 冲突或没有预留 Repair Slot 的 Instruction Set，
以及无法装入耐久 Superstep 范围的组合。它不会静默降级为更弱的行为。

生成的 Graph 有两个稳定的可执行 Node：

1. `agent.model` 从不可变 Invocation Ledger 重建完整 Provider-native Transcript，
   Prepare 或恢复一个 Model Attempt，并产生最终 Model-native Output 或提交 Tool Route。
2. `agent.tools` 依据已 Admission 的 Descriptor 解析每个 Proposal、校验 Arguments、
   执行固定版本 Policy，再通过已配置的 Ordered Pipeline 执行或恢复 Tool，最后返回
   `agent.model`。

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

## 有序 Parallel Tool Wave

`AgentToolConcurrency::sequential()` 保持逐个执行；
`parallel_read_only(max_concurrency)` 把一次 Model Response 划分为最大的连续 Read-only
Wave，并按有限并发值切分。Risk 只来自不可变、已 Admission 的 `ToolDescriptor`，绝不
相信 Model Output 或 Provider Annotation。每个 Idempotent/Non-idempotent Write 都是一个
Singleton Barrier：前面的 Read 全部落盘后才能启动，后面的 Read 必须等它提交 Terminal
Fact 后才能开始。

```rust,ignore
let execution = AgentExecutionConfig::new(
    AgentStructuredOutputStrategy::ModelNative,
    max_model_turns,
    ExecutionCount::new(1),
    max_tool_calls_per_turn,
    AgentToolConcurrency::parallel_read_only(ExecutionCount::new(8)),
)?;
```

并发值应依据 Provider Quota、Connection Pool Capacity 与最大已 Admission Response
实测确定，不能直接复制示例值。

每个 Read-only Wave 会先按 Provider Proposal 顺序校验 Policy/Arguments、Prepare Logical
Invocation，并串行提交 Physical Start；只有之后 External Provider Call 才能重叠。完成
Evidence 保留在内存，并按原 Proposal 顺序串行 Commit，因此任务完成时序不会改变 Journal
或 Model Transcript。若后续 Launch 失败，所有已经启动的 Call 仍会先经过同一条有序
Terminal Path，再返回 Launch Error。Cancellation 不会遗留 Detached Provider Task：
Child Call 会随所属 Graph Node 一起 Abort，而 Durable Start 仍可由 Fenced Supervisor
观察。

该模式只并行 Provider I/O，不会削弱 Durable Start Authority、Tool Ambiguity、Schema
Validation、Budget Accounting 或 No-redispatch Recovery。进程在 Start 后、Terminal
Persistence 前崩溃时继续 Fail Closed：StateKnot 不会猜测丢失的 Read Result，也不会盲目
重复 Write。

## 从耐久证据修复 Structured Output

Output Repair 是显式、有界的 Model Self-loop，不是 Adapter 内部 Retry。
`max_output_repair_turns` 表示一个 Run 最多可以额外消耗多少次付费 Model Turn；每次
Repair 仍同时受总 Model Turn、Token、Byte、Cost、Deadline 与 Lease 上限约束。

StateKnot 只会根据以下两类精确 Terminal Fact 启动 Repair：

- 已提交的 `Completed` Response 未包含唯一且满足已 Admission Output Schema 的 JSON；
- Complete-response Adapter 在 `Response` Phase 失败，稳定错误码为
  `response.malformed`，并且带有精确的标准化 Usage Snapshot。当第一方 OpenAI
  Responses 或 Anthropic Messages Adapter 能识别 Provider Usage、但无法接受其
  Structured Output 时，就会产生这种 Failure Shape；无法取得 Usage 的损坏 Envelope
  仍作为普通 Model Failure Fail Closed。

失败 Attempt 必须先提交。随后，其精确 `committed` 或 `failed` Model Revision 会绑定进
Node Result；Successor Checkpoint 只保存对应 Invocation ID，并为下一轮生成全新的
Logical Invocation ID 与 Physical Attempt ID。Crash 后，Replay 会加载此前 Terminal
Ledger 并推进到新 Plan，不会重新 Dispatch 已损坏的 Attempt，也不会重复计费。

Repair Request 会重建原始 Input 与可信 Instruction，再附加一个 Framework-owned、名为
`stateknot.output_repair` 的 Instruction。无效 Payload 与 Provider Error Text 不会复制
进 Prompt 或 Model Transcript；Repair Request 不发布任何 Tool，并把 Tool Selection
设为 `none`。Compile 会预留 32 个 Instruction Slot 中的一个，并拒绝
应用占用该保留名称，因此 Deployment 无法覆盖 Repair Policy。

Provider 在 Repair 期间返回的 Tool Proposal 属于无效 Output。StateKnot 不调用 Tool Policy，也不会
Prepare 或 Dispatch Tool；该 Proposal 会消耗当前 Repair Turn。达到配置上限后，执行以
`runtime.agent.output_repair_exhausted` 失败，Lifecycle Evidence 会精确报告已计费的
Attempt 与 Turn。

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
Outcome 绝不会作为普通 Business Call 自动 Retry。当不可变 Descriptor 与精确安装的
Provider 都启用 Reconciliation 时，Tool Node 会针对原 Physical Attempt 执行一次有界
Probe；权威 Evidence 原子提交，`Pending` 则转换为后续 Lease 下的耐久 `SafeAfter`
Node Retry。否则 Run 会保持阻塞，等待显式人工 Reconciliation。

每个 Tool Plan 都从已经持久化的不可变 Identity 确定性派生 Reconciliation Audit
`EventId`，不增加 Checkpoint 字段或 State Schema Version；升级前已经 Admission 的 Graph
Reference 因而保持完全相同的 Wire 与 Digest Compatibility。

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
- 两个 Read-only Call 真实重叠并乱序完成，后续 Write 形成 Barrier，下一轮 Model 仍按
  原始 Proposal 顺序读取 Transcript；
- Unknown Tool Outcome 返回 `Pending` 后耐久延迟，在后续 Lease 下完成解析并继续下一轮
  Model；两次 Reconciliation Probe 期间只发生一次 Business Call；
- 无效的已提交 JSON 会写入带全新 Invocation Identity 的有限 Repair Plan，并在新 Lease
  下恢复且不重复 Dispatch；
- 带精确 Usage、兼容第一方 Adapter 的 `response.malformed` Failure 会作为 Failed Model
  Revision 绑定，从 Checkpoint 修复，并按一个付费 Turn 计量；
- Repair Exhaustion 会精确累计 Usage；
- Repair 期间的 Tool Proposal 不会到达 Policy 或 Tool I/O；
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

本里程碑尚未交付 Parallel Write、Loop/Subgraph 语义、通用 Artifact
Lifecycle/Public Delivery、稳定 Network Agent/Cancellation Transport、Protocol-specific Outbox
Dispatch、MCP/A2A Server Composition、更广 Protocol Extension、A2A Live-peer
Reconciliation Qualification、Live-provider Drift Cassette、数据库 Role Separation、通用
Retention、Failover/Restore Qualification 或生产 Release。
[`AgentServiceV1`](agent-service.zh-CN.md) 现在提供嵌入式 Service Boundary，
[`McpRemoteTool`](mcp-remote-tool.zh-CN.md) 提供严格 Client-side Tool Profile，
[`A2aRemoteAgent`](a2a-client.zh-CN.md) 提供耐久 Outbound Agent Tool Profile。独立
[MCP Server Profile](mcp-server.zh-CN.md) 与 [A2A Server Profile](a2a-server.zh-CN.md)
暴露自身 Application Boundary；它们都不扩大 Provider-native Graph 声明。这些能力
仍需要独立版本化契约与可执行证据，不能从 Provider-native Graph 推断得出。
