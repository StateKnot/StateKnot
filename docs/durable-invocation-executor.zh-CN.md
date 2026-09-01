<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 耐久 Model/Tool 调用执行

`stateknot-runtime` 现在包含耐久 Model/Tool Ledger 与外部 Adapter
之间的 Provider-neutral 执行边界。它仍是未发布的 pre-alpha。本文件记录代码已经执行的集成与恢复合约；它不代表 OpenAI、Anthropic 或 MCP Adapter 已经交付。

英文版见 [Durable model and tool invocation execution](durable-invocation-executor.md)。

## 已实现边界

Runtime 当前提供：

- 启动阶段构建的不可变 `ModelProviderRegistry` 与
  `ToolProviderRegistry`，以精确的 Owner/Name/Version Capability Identity
  为键；
- Dispatch 前同时校验耐久 Invocation、启动快照和当前 Object-safe Provider
  的完整 Descriptor；
- 可信 `InvocationBudgetProvider` 边界：从耐久 Run Provenance
  解析有限剩余额度，不接受调用者自行填写的 Remaining Budget；
- 成对的墙钟与单调时钟观测，分别用于耐久记账决策和活动 Deadline；
- 带稳定 Start/Terminal Event Identity 的 Durable-before-dispatch
  Model/Tool `StartAttempt` Commit；
- Unary Model、语义 Streaming 校验与累积，以及必需的耐久 Stream Event Sink；
- Tool 执行，并对 Write Effect 的 Cancellation/Deadline 歧义做显式分类；
- 使用相同事务意图的有界 PostgreSQL 重试与精确 Lost-ACK Convergence；
- 保留 Terminal Handoff，只重试持久化而不重复 Provider/Tool I/O；
- 关闭的 Public-safe Journal Schema，明确排除 Prompt、Argument、Response、Error、Endpoint Identity 与 Credential。

Model 或 Tool 代码运行期间不会保持数据库事务。

## 启动时冻结精确 Provider

只有在 Descriptor 和 Schema 已验证后才能注册 Adapter。每次部署构建新的不可变 Registry Snapshot；活动 Worker 下方的 Provider Selection 不得动态变化。

```rust,ignore
let mut model_bindings = ModelProviderRegistryBuilder::new();
model_bindings.register(model_adapter)?;

let mut tool_bindings = ToolProviderRegistryBuilder::new();
tool_bindings.register(tool_adapter)?;

let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
register_standard_invocation_execution_event_schema(&mut schemas)?;
register_application_schemas(&mut schemas)?;

let executor = DurableInvocationExecutor::new(
    store,
    schemas.build()?,
    model_bindings.build(),
    tool_bindings.build(),
    budget_provider,
    DurableInvocationExecutorOptions::default(),
)?;
```

Alias、Model Family、可变 Endpoint Routing 和 Fallback Selection
必须在 Invocation Intent 持久化前解析完成。恢复只接受耐久记录中的精确 Descriptor；缺失或变化的 Binding 会在外部 I/O 前 Fail Closed。

## 执行一个物理 Attempt

调用方需要保留一个 `ModelAttemptHandoff` 或 `ToolAttemptHandoff`，其中包含：

- 精确且仍存活的 `RunFence`；
- Prepared 或已被显式允许重试的耐久 Invocation Revision；
- 新的 Run-wide 物理 `AttemptId`；
- 分离且稳定的 Start/Terminal `EventId`；
- Cooperative Cancellation Signal；
- 必需的 Model Stream Sink 或可选 Tool Progress Sink。

使用该 Handoff 调用一次 `execute_model` 或 `execute_tool`。Executor 会：

1. 校验 Tenant/Run Scope 与可启动 Ledger State；
2. 检查该物理 Attempt 是否已经推进耐久 Ledger；
3. 解析精确 Provider 与可信 Remaining Budget；
4. 提交 `StartAttempt` 及其 Journal Event；
5. 只有全新的 `Committed` 结果才获得 Dispatch 权限；
6. 校验 Provider 的完整 Response、Stream、Result 或 Error；
7. 对精确 Invocation Head 提交 Terminal Fact。

以 Idempotent 方式观察到的 Start 只返回 `Recovered`，绝不再次调用外部 Adapter。它可能表示数据库 ACK 丢失，也可能表示另一个 Executor，因此不是新的 Dispatch Authority。

## Streaming 合约

Streaming Request 必须携带 `Arc<dyn ModelEventSink>`。Runtime
按顺序验证并累积每一个语义 `ModelEvent`，等待 Sink 接受该精确 Event
后才继续 Poll。Sink 实现必须先按 `(attempt_id, sequence)` 耐久去重，再向外部暴露 Event。

累积出的 `ModelResponse` 只有在独立的 Terminal Ledger Commit
成功后才成为权威结果。缺少 Terminal Stream Event、Sequence 违规、无效 Provider Error、Sink Failure、Cancellation 或 Deadline 都会形成 Public-safe Model Error，不会形成成功 Response。

## Tool 歧义绝不转换成盲目重试

Read-only Tool 可以记录结果已知的 Cancellation 或 Deadline Failure。对于 Idempotent/Non-idempotent Write，取消、超时或无效 Failure Contract
可能发生在外部状态已改变之后。Executor 因此记录：

- `FailureCategory::AmbiguousExternalOutcome`；
- `ToolExternalEffect::Unknown`；
- `RetryAdvice::ReconcileFirst`。

耐久 Tool Ledger 保持 `Unknown`，直到应用自己的 Reconciliation
确认外部结果。Executor 不会因为本地 Future 被丢弃就再次调用 Tool。

Provider SDK 的隐藏重试同样必须关闭，除非 Adapter 能证明它复用精确 Provider Request Identity，并满足耐久 Descriptor 声明的语义。StateKnot 本身不会隐藏一次外部重试。

## 不重复 Dispatch 的 Terminal Commit 恢复

如果 Provider I/O 已完成，但 Terminal Database Commit 无法确认，
`execute_model` 或 `execute_tool` 返回拥有精确、Payload-redacted Recovery Handoff 的 Terminal Error。

```rust,ignore
match executor.execute_model(handoff).await {
    Ok(outcome) => consume(outcome),
    Err(ModelAttemptExecutionError::Terminal(error)) => {
        let recovery = error.into_recovery();
        persist_for_immediate_retry(recovery);
    }
    Err(error) => handle_pre_dispatch_failure(error),
}
```

只能把该值传给 `commit_model_terminal` 或 `commit_tool_terminal`；两者都不会执行 Provider I/O。如果原 Lease 在外部调用期间过期，先取得同一 Tenant/Run 的当前 Live Fence，再调用 `rebind_fence`。Store 仍会做权威的 Live-fence 校验。

Terminal Recovery Payload 含应用数据，因此有意不支持序列化或打印。只在可信进程内存中保留，并在有界 Lease Recovery Workflow 内立即重试。若进程在外部完成后、Terminal 持久化前崩溃，恢复从耐久 Executing Ledger 开始：Write Tool 必须 Reconcile；Model 是否可重新尝试由应用根据持久化 Retry Contract 决定。

## Budget 与 Deadline 所有权

`InvocationBudgetProvider::remaining` 必须重新加载已 Admission Run
的不可变 Budget 和累计耐久 Usage，对精确 Invocation/Attempt 执行 Policy，并在给定可信时间返回有限 `BudgetRemaining`。Executor 在 Durable Start
前检查 Model Attempt/Turn/Token/Byte Capacity，以及 Tool/Write-call Capacity。

已启动 Attempt 的恢复发生在 Provider Lookup、Clock Access 和 Budget Evaluation
之前。这个顺序不可改变：即使部署已变化，或原始 Start 后额度已消耗，Lost ACK 仍必须可以恢复。

## 公开 Journal Schema

通过 `register_standard_invocation_execution_event_schema` 安装 Schema。其不可变 Identity 为：

```text
https://stknot.com/schemas/runtime/invocation-execution-event/1.0.0
```

六种 Operation 覆盖 Model/Tool Start，以及 Terminal Response/Result/Error Fact。每个 Event 只暴露 Binding Kind、Logical Invocation ID、Physical Attempt ID 和 Intent Digest。应用 Payload 保留在各自有界 Ledger 中。

## 运维要求

至少观测：

- 按 Boundary Kind 分类的 Admission、Durable Start、No-dispatch Recovery 与 Terminal Commit；
- Provider Duration、Deadline/Cancellation Result 与 Contract Violation；
- 已接受 Stream Event 与 Durable Sink Failure；
- 等待 Reconciliation 的 Ambiguous Tool Outcome；
- Mutation Retry 与保留的 Terminal Handoff；
- Exact-provider Lookup 与 Budget-provider Failure。

若 Terminal Handoff 在 Lease Recovery 前仍无法 Commit、出现任何 Contract Violation，或 `Unknown` Tool Invocation 持续增长，应告警。禁止从 Recovery Handoff 记录 Descriptor Secret、Request/Input Byte、Response/Result、Model Error 或 Tool Error。

## 验证证据与剩余 Blocker

真实 PostgreSQL Integration Coverage 已证明：

- Model Call 期间原 Fence 被取代后，Terminal Evidence 会被保留、Rebind 到新 Live Fence、只 Commit 一次，并且后续重试不重新计算 Budget、不重新 Dispatch；
- 七个语义 Model Stream Event 按序到达耐久 Sink，累积为已提交 Response，Duplicate Recovery 不再产生 Event 或 Provider Call；
- Timed-out Idempotent-write Tool 记录 Ambiguous/Reconcile-first Outcome，重试时不会再次调用 Tool。

该边界仍是 pre-alpha。第一方 OpenAI Responses/Anthropic Messages Adapter 与强类型 Agent
Contract 与原子 Admission 现已实现；应用持久化 Model Stream Sink、生产
Accounting/Result Evidence、在预置 Graph 内耐久组装 Transcript、完整 Crash Reconciliation
Supervision、Telemetry 与 Live-provider Qualification 尚未完成，因此还不能声称生产支持。
