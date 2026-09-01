<!-- Copyright 2026 StateKnot contributors -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# 强类型 Agent 与第一方 Model Adapter

本文只描述已经实现的 pre-alpha 边界。它刻意不是“一行 `run()`”教程：StateKnot
不会让便利 API 绕过耐久 Admission、Attempt Ledger、Graph Driver、Lifecycle
Evidence 或 Tenant Scheduling。

## 已经实现的能力

`stateknot-runtime` 现在提供：

- `AgentBuilder<I, O>`：分别根据 `I` 的序列化合约与 `O` 的反序列化合约生成
  JSON Schema 2020-12；
- 对两个生成文档执行 RFC 8785 Canonicalization 与 SHA-256 固定；
- `TypedAgentDefinition<I, O>`：成对注册生成的 Schema，不向调用方暴露只注册一半的
  Builder；
- 启动期 Binding：要求 Agent、Tool、Model Profile 引用的每一个 Schema 都存在于同一个
  不可变离线 Registry，并用固定的 Provider Profile 校验所有 Model-visible Schema；
- `TypedAgent<I, O>::prepare_request`：在耐久 Admission 前完成有界 JSON
  序列化与本地 Input Schema 校验；
- `TypedAgent<I, O>::decode_result`：在反序列化 `O` 前重新校验可信
  Provenance、Request Binding、完整 Budget/Accounting Evidence 与 Output Schema。

`stateknot-integrations` 现在提供两种生产形态的 Binding：

- OpenAI Responses / OpenAI-compatible Responses Endpoint；
- Anthropic Messages Endpoint。

两者都使用 Attempt-scoped Credential；除显式的 Literal-loopback 测试外强制 HTTPS；
关闭 HTTP Redirect 与隐藏 Client Retry；限制 Request、Response 与 SSE 资源；遵守协作式
Cancellation 与单调 Deadline；保留 Provider Request ID；规范化 Usage；只返回公开安全的
错误。Adapter Diagnostic 不会格式化 API Key、Provider Body、Prompt 文本或 Model Output。

## 编译并运行本地示例

以下示例不会请求 Provider，也不需要 Credential：

```console
cargo run -p stateknot-runtime --example typed_agent
cargo run -p stateknot-integrations --example provider_adapters
```

Workspace 的 `--all-targets` CI 门禁也会编译它们。完整源码：

- [`crates/stateknot-runtime/examples/typed_agent.rs`](../crates/stateknot-runtime/examples/typed_agent.rs)
- [`crates/stateknot-integrations/examples/provider_adapters.rs`](../crates/stateknot-integrations/examples/provider_adapters.rs)

类型化流程保持显式：

```rust,ignore
let definition = AgentBuilder::<IncidentRequest, IncidentReport>::new(
    agent_metadata,
    input_schema_id,
    output_schema_id,
    model_descriptor,
    trusted_instructions,
    execution_limits,
)
.build()?;

let mut schemas = JsonSchemaRegistryBuilder::default();
schemas.register(provider_profile, provider_profile_document)?;
let schemas = definition.register_schemas(schemas)?.build()?;
let agent = definition.bind(Arc::new(schemas))?;

let request = agent.prepare_request(&input, request_budget_limits)?;
// Durable Admission 随后分配 tenant/run/thread/invocation identity，
// 冻结 ResolvedBudget，再由 DurableAgentLoop 执行已经 Claim 的 Graph。
```

`register_schemas` 会移入 Startup Builder，只有两个生成资源都被接受才会返回它。
失败时调用方看不到只安装了 Input 或 Output 的半成品 Registry。

## Provider 合约

| 边界 | OpenAI Responses | Anthropic Messages |
| --- | --- | --- |
| 完整响应 | 已实现 | 已实现 |
| 真正增量 SSE | 已实现 | 已实现 |
| 文本输入/输出 | 已实现 | 已实现 |
| JSON Schema 输出 | 已实现 | 已实现 |
| Function/Tool Proposal | 已实现 | 已实现 |
| 本地参数/输出校验 | 已实现 | 已实现 |
| Provider-native Unary Tool 续接 | 已实现 | 已实现 |
| Generic JSON Mode | 已实现 | 拒绝：稳定合约要求 Schema-constrained Output |
| 可读 Reasoning Summary | Binding 显式声明时可用 | 当前 Adapter 不声明 |
| 旧式 `role=tool` Message | I/O 前拒绝 | I/O 前拒绝 |
| Artifact/多模态输入 | I/O 前拒绝 | I/O 前拒绝 |
| Request Extension | I/O 前拒绝 | I/O 前拒绝 |

`ModelTranscript` 是无损续接合约。每个 Turn 会把一个规范化 `ModelResponse`、精确且有界的
Provider Replay Fragment，以及按 Provider 顺序与每个 Proposal 一一对应的已提交
`ModelToolOutcome` 绑定在一起。构造与反序列化会拒绝缺失或重复的
Attempt/Call/Invocation Identity、Tool Version 漂移、外部副作用不确定、Payload 被替换及资源
超限。发起网络 I/O 前，所选 Adapter 还会校验 Model/Provider Binding 与 Replay Format，
重新解析不透明 Fragment，并要求它与规范化 Response 完全一致。

OpenAI 会先完整回放既往 `response.output`，再附加对应的 `function_call_output`；启用 Tool
时还会请求加密 Reasoning 续接块。Anthropic 会完整回放 Assistant Content，随后紧接一个
User Message，其中包含全部按序 `tool_result` Block。普通 `role=tool` Message 仍会被拒绝，
因为它无法证明任一 Provider-native 顺序合约。产生 Tool 的 Streaming Attempt 暂时不会输出
Replay Evidence；在 Streaming Event 合约能够携带精确续接快照前，耐久 Agent Tool Loop
必须使用 Complete Response。

Streaming Adapter 不会先缓存完整响应，再伪装成 Chunk 重放。它们会增量解析有界 SSE；
每个发出的 Event 都先经过 Core `ModelEventAccumulator`；Channel Backpressure 有界；
并且必须看到成功 Terminal Event。OpenAI 的 Terminal Response Snapshot 还会与每个已经
流出的语义 Item 交叉校验。截断、重复、重排或替换的 Terminal Data 都不会发出
`Completed`。

## 耐久执行边界

`TypedAgent` 是类型化合约与 Codec，不是进程内 Runner。受支持的路径仍然是：

1. 认证并选择不可变 Agent/Model/Tool Descriptor Snapshot；
2. 离线校验类型化 Request 与全部 Digest-pinned Schema；
3. 把 System、Tenant、Policy、Agent、Request Limit 解析为一个有限
   `ResolvedBudget`，再通过 `DurableAgentAdmission` 原子提交不可变 Intent、Initial
   Event 与 Superstep-zero Checkpoint；
4. 用 Lease/Fence Claim Graph；
5. 通过 `DurableInvocationExecutor` 执行 Model/Tool Attempt，在外部 Dispatch 前先提交
   Attempt Start；
6. 通过 `DurableAgentLoop` 驱动 Checkpoint 与 Lifecycle Handoff；
7. 使用 `TypedAgent` 校验并解码 Terminal `AgentResult`。

原子耐久 Admission 已经实现，详见
[`durable-agent-admission.zh-CN.md`](durable-agent-admission.zh-CN.md)。当前不存在把
“映射外部 Idempotency Key、生成预置 Model/Tool Graph、执行、读取 Terminal Result”压缩成
一次调用的公开 Helper。剩余集成必须携带 Result Retrieval Evidence，因此不会用临时
In-memory `run()` 冒充。

## 验证证据

Adapter Suite 使用真实本地 TCP HTTP Server，而不是 Mock Provider SDK。目前覆盖 Request
Header/Body、Usage Normalization、碎片化 SSE、支持 Backpressure 的增量 Event、Terminal
交叉校验、截断、重复 JSON Member、Response 替换、429 `Retry-After`、5xx 无隐藏 Retry、
Request/Response Byte Ceiling、Credential Deadline、Cancellation 优先级，以及 Secret/Debug
Redaction。

执行：

```console
cargo test -p stateknot-integrations --all-targets
cargo test -p stateknot-runtime --test typed_agent
```

Live-provider Qualification、Provider Drift Cassette、在预置 Agent Graph 内耐久组装
Transcript、Policy Middleware 与完整公开 Run/Result Facade 仍是发布门禁。Adapter 与
类型化 API 已经实现，但仍处于未发布的 pre-alpha。
