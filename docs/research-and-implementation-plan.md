<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot：Rust 生产级 Agent 框架调研与实现方案

> 状态：架构评审草案
> 调研基线：2026-08-28
> 目标：形成可长期演进、可生产部署的 Rust Agent 框架，而不是对某个 Python 框架的接口翻译。

## 1. 结论先行

项目定位为：

> **Rust-native、typed、durable、protocol-native、policy-enforced 的 Agent 应用框架与运行时。**

它应同时提供两种使用方式：

1. 作为 Rust library 嵌入业务服务；
2. 作为独立的 API/worker 服务部署，承担长任务、分布式执行、暂停恢复和协议网关。

核心决策如下：

- 不复刻 LangChain API。借鉴 LangGraph 的状态图、superstep、checkpoint 与 interrupt 语义，但重新设计 Rust 类型系统、错误模型、并发语义和持久化边界。
- 框架必须拥有自己的稳定领域模型。Rig、官方 A2A/MCP SDK、各模型 SDK 都只能位于 adapter 层，不能成为公共 API 的事实标准。
- 首发正式协议为 **MCP 2026-07-28 client/server** 与 **A2A 1.0 client/server**；协议版本必须协商、可测试、可独立升级。
- AG-UI、MCP Apps、Agent Skills 放入第二优先级；A2UI、AGNTCY/SLIM、AP2 先保留扩展点并跟踪成熟度，不能把候选规范直接固化进核心模型。
- 默认生产运行时采用 **PostgreSQL journal + checkpoint + lease/fencing + outbox**。内存实现只允许用于开发与测试。
- Restate 可作为可插拔耐久执行后端；由于 Rust SDK 仍声明可能发生破坏性变更，不应成为框架的强制依赖。Temporal Rust SDK 仍处于预发布/公开预览，也不适合作为默认基座。
- 对数据库内状态提供事务性一次提交；对外部工具副作用只承诺现实可实现的语义：**至少一次执行 + 幂等键 + 调用账本 + 可选补偿**。不宣传无法兑现的“所有工具 exactly-once”。
- 安全、可观测性、评测、成本预算、人工审批、租户隔离和崩溃恢复都属于核心能力，不是后续插件。
- 项目源代码、文档、示例与正式发布物统一采用 **Apache License 2.0（SPDX：`Apache-2.0`）**；第三方内容按其原许可证保留归属与 NOTICE，不以该许可证暗示项目隶属于 Apache Software Foundation。

## 2. 产品边界

### 2.1 v1 必须覆盖

- 文本、图片、音频、文件引用等多模态消息与 artifact；
- 模型调用、流式响应、结构化输出、工具调用与 provider capability negotiation；
- 可直接使用的 agent loop，以及 router、handoff、supervisor、agents-as-tools 等组合模式；
- typed graph、条件分支、循环、并行、join/reducer、子图、暂停/恢复、取消、重试和故障恢复；
- thread-scoped checkpoint 与跨 thread 长期 memory 的明确分离；
- PostgreSQL 持久化、多 worker 抢占、租约、fencing、事件流恢复；
- MCP client/server 与 A2A client/server；
- HTTP API、SSE 事件流、认证授权、审计、OpenTelemetry、离线/在线 eval；
- 确定性 fake model、故障注入、协议契约测试和 conformance test 集成。

### 2.2 v1 明确不做

- 低代码可视化编排器；
- 覆盖所有模型供应商、向量数据库和文档连接器；
- 自研模型训练/微调平台；
- 允许任意第三方代码在主进程内执行的插件市场；
- 在 Rust API 稳定前发布另一套 YAML/JSON DSL；
- 自研通用分布式数据库或工作流数据库；
- 把“自主发现并信任互联网上任意 Agent”作为默认行为。

这些边界不是临时妥协，而是为了让 v1 的承诺可以被测试、运维和长期维护。

## 3. 多框架调研

### 3.1 跨语言框架

| 项目 | 最值得借鉴的能力 | 不应直接照搬的部分 |
|---|---|---|
| [LangChain / LangGraph](https://docs.langchain.com/oss/python/langgraph/persistence) | StateGraph、BSP/Pregel 式 superstep、checkpoint、pending writes、interrupt、time travel、短期与长期记忆分离 | Python 动态类型、庞大兼容层、回调与配置对象扩散 |
| [Pydantic AI](https://pydantic.dev/docs/ai/core-concepts/agent/) | typed dependencies/output、模型能力声明、toolset、durable adapter、eval 与 graph builder | 依赖 Python/Pydantic 运行时的模式 |
| [OpenAI Agents SDK](https://openai.com/business/guides-and-resources/a-practical-guide-to-building-ai-agents/) | 少量核心原语、agent/tool/handoff/guardrail/tracing、易用的预置 loop | 供应商专有模型语义不能进入核心接口 |
| [Google ADK](https://google.github.io/adk-docs/) | LLM/Sequential/Parallel/Loop Agent、session/state/memory/artifact、callback/plugin、A2A/MCP、eval/deploy | 与 Google 服务绑定的部署和 provider 假设 |
| [Microsoft Agent Framework](https://learn.microsoft.com/en-us/agent-framework/) | Pregel 风格 workflow、superstep checkpoint、A2A/MCP/AG-UI，以及“跨边界才使用 A2A”的清晰原则 | 当前官方 SDK 不含 Rust；不依赖社区 Rust 移植作为核心 |
| [AutoGen](https://github.com/microsoft/autogen) | event-driven core、team/swarm/selector/magentic 等多 Agent 协作模式 | 项目已进入 community-managed maintenance，新项目被官方引导至 Microsoft Agent Framework；其状态保存与实验性 graph 也不能作为耐久语义基线 |
| [CrewAI](https://docs.crewai.com/en/concepts/flows) | role/process 的高层易用性、event-driven flow、分支/循环/HITL | “角色扮演”抽象不应替代明确的数据流、权限与执行保证 |
| [LlamaIndex](https://docs.llamaindex.ai/en/stable/) | ingestion/retrieval/query、agents-as-tools、结构化输出，是 RAG 层的重要参考 | 不在 v1 复制其庞大 connector catalog |
| [Mastra](https://mastra.ai/articles/ai-workflows) | TypeScript 下清晰的 chain/branch/parallel/suspend-resume、snapshot、OTel 与 eval 体验 | JS/TS 特有 API 形态和运行时假设 |

综合判断：没有任何一个项目同时把 Rust 类型安全、耐久执行、A2A/MCP、生产治理和易用 API 做完整。因此项目有明确价值，但差异点不能只是“Rust 版 LangChain”。

### 3.2 Rust 原生生态

| 项目 | 判断 | 本项目策略 |
|---|---|---|
| [Rig](https://rig.rs/) | 当前 Rust Agent 生态里 provider、tool、RAG 和 workflow 覆盖较全面；但仍在 1.0 前，近期持续拆分核心契约，durable pause/session 仍在演进 | 作为 API 和生态对照；v1 不引入 bridge，公共 trait 不依赖 Rig 类型 |
| [Swiftide](https://github.com/bosun-ai/swiftide) | typed task、streaming RAG、agent、MCP、HITL 和 tracing 值得参考；项目明确处于 1.0 前 | 参考 RAG pipeline 与 task ergonomics，不作为运行时基座 |
| [agent_graph](https://docs.rs/agent-graph/latest/agent_graph/) | LangGraph 风格，但版本和生态成熟度不足 | 不作为依赖，只作 API 对照 |
| [agent-framework-rs](https://github.com/CodeHalwell/agent-framework-rs) | 能力面较广，但不是 Microsoft 官方 Rust SDK | 仅作功能比较，不进入信任链 |
| [a2a-rs](https://github.com/a2aproject/a2a-rs) | A2A 官方 Rust SDK，覆盖 client/server/JSON-RPC/REST/gRPC 等 | adapter 内优先采用并锁定版本，同时用官方 TCK 防回归 |
| [MCP Rust SDK](https://github.com/modelcontextprotocol/rust-sdk) | 官方 SDK，已跟进 2026-07-28；Rust 支持仍标为 beta | adapter 内采用；禁止其 wire/domain 类型穿透公共 API；CI 跑官方 conformance suite |

### 3.3 耐久执行引擎

| 方案 | 优点 | 风险/限制 | 决策 |
|---|---|---|---|
| 自有 PostgreSQL runtime | 无外部控制面依赖；可嵌入；事务、RLS、备份、观测和运维成熟；可精确实现 graph 语义 | 调度、租约、迁移和故障测试工作量大 | v1 默认生产基线 |
| [Restate](https://docs.restate.dev/ai/patterns/durable-agents) | journal-based durable execution、signal、workflow/object、集群与生产部署能力较完整；有原生 Rust SDK | 增加独立运行时；Rust SDK 仍提示跨版本可能破坏；语义会约束框架 API | v1 预留 runtime SPI，后续提供正式 adapter |
| [Temporal Rust SDK](https://github.com/temporalio/sdk-rust) | Temporal 生态与耐久 workflow 理念成熟 | Rust SDK 仍是 public preview / prerelease | 观察，达到稳定与 conformance 门槛后再做 adapter |
| 纯内存 / SQLite | 本地开发方便 | 不支持真正的 HA、多 worker 与可靠恢复 | 仅 dev/test，文档中不得标为生产后端 |

## 4. 前沿协议地图与优先级

协议必须按职责分层，不能把所有概念塞进同一个“Agent Protocol”结构：

```text
用户/前端          AG-UI · A2UI · MCP Apps
       │
Agent 运行时       内部 Run / Event / Artifact / Interrupt 模型
       │
Agent ↔ Agent      A2A 1.0；未来可接 AGNTCY discovery / SLIM transport
       │
Agent ↔ 能力       MCP 2026-07-28 · local tools · skills
       │
领域扩展           AP2 等支付/商业协议
```

| 协议/标准 | 当前作用与成熟度 | v1 决策 |
|---|---|---|
| [A2A 1.0](https://a2a-protocol.org/latest/specification/) | Agent Card、message/task/artifact、流式、订阅、push notification；JSON-RPC、HTTP+JSON、gRPC；已有官方 Rust SDK/TCK | **P0，正式支持**。REST 与 JSON-RPC 先达到生产门槛，随后补 gRPC；实现完整 task 生命周期与 version negotiation |
| [MCP 2026-07-28](https://blog.modelcontextprotocol.io/posts/2026-07-28/) | 最新核心转向 stateless；工具 schema 使用 JSON Schema 2020-12；Task 被拆为扩展；旧 HTTP+SSE、roots/sampling/logging 等进入弃用路径 | **P0，正式支持**。client/server、tools/resources/prompts；Task extension 独立 feature；不围绕已弃用原语新增核心设计 |
| [AG-UI](https://github.com/ag-ui-protocol/ag-ui) | Agent 到用户界面的事件协议，覆盖消息、tool、state、HITL；事件集合仍演进，Rust 实现主要来自社区 | **P1，Beta adapter**。映射内部 event journal，不反向污染 runtime 语义 |
| [MCP Apps](https://modelcontextprotocol.io/extensions/apps/overview) | MCP 首个正式扩展，让 tool 返回 sandboxed interactive UI；官方称已可生产使用，但 client 支持不完全一致 | **P1**。server 端资源和安全策略先行，host renderer 单独交付 |
| [Agent Skills](https://www.anthropic.com/engineering/equipping-agents-for-the-real-world-with-agent-skills) | 可移植的 `SKILL.md` + scripts/resources，适合渐进加载程序性知识，与 MCP 互补 | **P1**。只加载受信来源；清单、权限、哈希、签名和 sandbox policy 必须进入设计 |
| [A2UI v1.0 Candidate](https://github.com/a2ui-project/a2ui/blob/main/specification/v1_0/docs/a2ui_protocol.md) | Agent 以 JSON stream 生成可更新 UI，规范仍标为 Candidate | **P2/实验**。先将其作为 artifact/event renderer，不设为稳定 API |
| [AGNTCY](https://github.com/agntcy) / [SLIM](https://github.com/agntcy/slim) | Agent directory、identity、OASF 与安全低延迟 transport；生态活跃但多项规范仍快速演进 | **P2**。预留 discovery/identity/transport SPI；不默认接入公共目录 |
| [AP2 v0.2](https://ap2-protocol.org/ap2/specification/) | Agent 支付授权、mandate、receipt 与争议证据，是领域协议而非通用 runtime 协议 | **领域扩展**。只有明确进入支付场景时实现，并要求确定性校验与签名验证 |

### 4.1 两条关键隔离规则

1. **内部 Run 不等于 A2A Task，也不等于 MCP Task。** 三者生命周期、授权范围和版本演进不同，adapter 必须显式映射。
2. **内部 Event 是唯一流式事实源。** SSE、A2A streaming、AG-UI、审计与 WebSocket 都从 append-only event journal 投影，避免多条流彼此不一致。

## 5. 生产级需求基线

### 5.1 正确性

- 相同 checkpoint、相同已记录外部结果和相同 graph version，调度与 reducer 结果必须确定；
- 并发节点按稳定键排序后进入 reducer，不依赖完成时序；
- 单个并行节点已提交结果后，即使同一 superstep 的其他节点失败也不重复执行；
- 任意等待态都可跨进程、跨 worker、跨部署重启恢复；
- cancel、deadline 与 backpressure 必须贯穿 model、tool、node 和 transport；
- graph 编译阶段拒绝不可达节点、缺失边、非法循环、未覆盖 route、schema 不匹配和不兼容版本。

### 5.2 可靠性

- run、event、checkpoint、interrupt、tool invocation、outbox 都是持久实体；
- worker 使用 lease + fencing token，过期 worker 的迟到写入必须被数据库拒绝；
- 所有跨系统通知通过 transactional outbox；consumer 以 event ID 幂等消费；
- retry 必须基于错误分类，而不是对所有错误统一重试；
- 支持限流、并发、深度、时长、token、费用和 tool-call 数量预算。

### 5.3 多租户与合规

- `tenant_id` 必须进入每个存储主键、索引、查询和 cache key；
- 认证与 scope 校验必须发生在读取 task/run 是否存在之前，避免 ID 枚举；
- prompt、工具参数、模型输出默认按敏感内容处理，日志记录采用显式 opt-in 与字段级脱敏；
- 每个外部动作可追溯到 user principal、agent、policy version、model/provider、tool version 和审批记录；
- 数据保留、导出、删除和 legal hold 通过 storage policy 实现。

## 6. 总体架构

```mermaid
flowchart TB
    SDK[Embedded Rust SDK] --> Core[Core domain model]
    API[Axum API / SSE] --> Runtime[Durable runtime]
    A2A[A2A client/server] --> Runtime
    UI[AG-UI / MCP Apps / future A2UI] --> Runtime
    Core --> Agent[Agent loop / routing / handoff]
    Agent --> Graph[Typed graph compiler + scheduler]
    Graph --> Runtime
    Runtime --> Journal[(PostgreSQL journal/checkpoints)]
    Runtime --> Blob[(Object/blob store)]
    Runtime --> Model[Model adapters]
    Runtime --> Tools[Local tools / MCP / remote A2A agents]
    Runtime --> Policy[Identity / policy / approvals / budgets]
    Runtime --> Telemetry[OTel + audit + eval events]
```

设计上分为五层：

1. **Domain**：稳定的 content、message、artifact、model、tool、agent、run、event、error、identity 类型；
2. **Orchestration**：agent loop、typed graph、router、handoff、supervisor、HITL；
3. **Durability**：journal、checkpoint、scheduler、lease、outbox、artifact store；
4. **Interop**：providers、MCP、A2A、UI protocols、discovery；
5. **Operations**：server、auth、policy、telemetry、eval、admin 与迁移工具。

## 7. Cargo workspace 规划

项目正式名为 `StateKnot`，crate 统一使用 `stateknot-*` 前缀。仓库初始化时只创建不发布的 `stateknot` facade；其他边界要在 RFC 和第一条纵向链路验证后才落为物理 crate：

| 候选 crate | 职责 |
|---|---|
| `stateknot` | 面向用户的 facade 与受控 prelude |
| `stateknot-core` | 公共领域类型、trait、错误、ID、capability、identity 与 budget |
| `stateknot-runtime` | agent loop、typed graph、scheduler、run 状态机、journal、checkpoint 与 recovery |
| `stateknot-integrations` | 模型 provider、MCP 与 A2A adapter；用 feature 隔离重依赖 |
| `stateknot-server` | HTTP/SSE、worker、scheduler、admin、health/readiness 与 graceful drain |
| `stateknot-testkit` | fake model/tool/clock、fault injector、test store 与协议契约测试 |

只有在编译时间、依赖隔离、独立发布或 semver 边界已被真实需求证明时，才继续拆分 store、graph、protocol 或 observability crate。`stateknot-macros` 只在手写 API 稳定后加入。

建议初始基线：Rust edition 2024，MSRV 1.85（与官方 A2A Rust SDK 基线兼容），workspace 统一 lockfile，默认禁止自有 crate 中的 `unsafe`。

## 8. 核心领域模型

### 8.1 Content / Message / Artifact

- core `ContentPart` 在 v1 封闭为 `Text`、bounded `Json`、`ArtifactRef`；image、audio、file 都通过带 MIME、长度、SHA-256、来源和安全标签的 `ArtifactRef` 表达；
- 大对象只保存 tenant-scoped `ArtifactRef`，不向领域对象暴露 URI、bucket、对象键、文件路径或永久公网 URL；
- `Message` 表示交互输入/输出，`Artifact` 表示任务产物，两者不能混用；
- provider 或协议未知字段只保存在 adapter 自己的有界、namespaced envelope 中，不能伪装为 core content 或绕过 schema/trust 策略；
- core durable wire 对未知安全枚举和值 fail closed；adapter 可在明确版本协商下保留未知协议字段，不能因供应商新增 event 崩溃或静默改变语义。

### 8.2 Model

公共接口不能只抽象成 `prompt -> String`。至少需要：

- `ModelCapabilities`：tool calling、parallel tools、structured output、vision/audio/files、reasoning summary、streaming、max context 等；
- request 中明确 system/developer/user/tool 层级、tool choice、response schema、sampling、budget、deadline 与 metadata；
- 统一 `ModelEvent`：message delta、tool-call delta/completed、usage、provider metadata、finish、error；
- adapter 在执行前做 capability negotiation，不支持时返回 typed error，不静默降级；
- 只暴露 provider 允许输出的 reasoning summary，不记录或转发隐藏思维链。

建议 v1 只提供两个高保真第一方 adapter：OpenAI Responses/OpenAI-compatible 与 Anthropic Messages。其他 provider 在有明确用户需求和契约测试后再加入。

### 8.3 Tool

工具规范至少包含：

- 唯一、版本化名称；描述；JSON Schema 2020-12 input/output；
- `read_only | idempotent_write | non_idempotent_write | code_execution` 风险分类；
- 所需 scopes、网络/文件/secret capability、timeout、并发和成本提示；
- 是否支持幂等键、取消、进度事件和补偿；
- 来源、签名/摘要、owner 与弃用信息。

```rust
pub trait Tool: Send + Sync {
    fn spec(&self) -> &ToolSpec;

    fn call(
        &self,
        ctx: ToolContext,
        input: serde_json::Value,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>>;
}
```

local function、MCP tool、workflow-as-tool 和 remote A2A agent 都可适配为可执行 capability，但必须保留各自身份、风险和生命周期，不能假装它们完全相同。

### 8.4 RunContext

`RunContext` 必须携带且向下传播：

- tenant、principal、delegation chain、scopes；
- run/thread/trace/correlation IDs；
- deadline、cancel token、budget、policy version；
- credential resolver 的句柄，而不是明文 secret；
- clock、random source 与 invocation ledger 访问器，便于确定性测试。

## 9. Agent 与 Graph 语义

### 9.1 预置 Agent loop

用户不应为最常见场景手写图。`AgentBuilder` 应组合 model、instructions、tools、output schema、memory、middleware、limits，直接得到可运行 Agent。

默认 loop：

1. 组装受信指令与上下文；
2. 调用 model；
3. 验证 tool call 与参数；
4. 经过 policy/approval；
5. 并行或串行执行工具；
6. 记录结果并继续，直到得到 final output 或达到预算；
7. 对结构化输出做 schema 校验，可配置有限次数的修复回合。

handoff 推荐表示为显式工具调用和 graph transition，并记录委派主体、scope 收窄、输入摘要与返回 artifact。

### 9.2 Typed graph

```rust
pub trait GraphState: Send + Sync + 'static {
    type Update: Send + 'static;
    fn reduce(&mut self, update: Self::Update) -> Result<(), StateError>;
}

pub trait Node<S: GraphState>: Send + Sync {
    fn run(
        &self,
        ctx: NodeContext,
        state: Arc<S>,
    ) -> BoxFuture<'_, Result<NodeOutcome<S::Update>, NodeError>>;
}
```

关键语义：

- 节点读取不可变 state snapshot，只返回 update、route、interrupt 或 terminal result；
- 同一 superstep 的节点并行执行，barrier 后按稳定顺序归并 update；
- reducer 必须显式，禁止对共享 `HashMap<String, Value>` 做隐式 last-write-wins；
- loop 必须声明最大迭代或预算；
- interrupt 恢复时允许节点重入，因此 interrupt 前的副作用必须通过调用账本保护；
- subgraph 有独立 schema/version，同时共享或隔离 checkpoint 由声明决定。

### 9.3 Run 状态机

内部状态建议为：

```text
Queued -> Running -> WaitingInput | WaitingApproval | WaitingAuth | Sleeping
                    -> Succeeded | Failed | Cancelled | Rejected
```

状态转换必须持久化为事件，并由乐观版本或 fencing token 防止并发写入。A2A 状态在 adapter 中映射，例如内部 `WaitingApproval` 可对外映射为 `INPUT_REQUIRED`，但内部信息不能因此丢失。

## 10. 持久化与故障恢复

### 10.1 采用 snapshot + journal，而非重放模型随机性

LLM 与外部工具天然不确定，不能假设“重新执行得到同样结果”。正确策略是：

1. journal 记录已完成的外部结果、路由决定、审批和状态变化；
2. checkpoint 保存 superstep barrier 后的完整 state；
3. 每个并行节点的 committed write 独立保存；
4. 恢复时重用已提交结果，只调度尚未完成的节点；
5. event journal 是流式恢复与审计的事实源。

### 10.2 推荐 PostgreSQL 实体

| 表 | 关键内容 |
|---|---|
| `runs` | tenant/run/thread、graph+version、status、budget、deadline、lease owner/epoch/expiry、输入输出摘要 |
| `run_events` | run 内单调 `seq`、event type、payload、schema version、checksum、created_at |
| `checkpoints` | superstep、parent、state blob/ref、state schema、graph version、hash |
| `node_attempts` | node、attempt、input hash、status、pending/committed update、错误与时间 |
| `tool_invocations` | invocation/idempotency key、risk class、prepared/executing/committed/unknown、外部引用和结果 |
| `interrupts` | kind、payload、required principal/scope、expiry、resolution 与 resolver |
| `outbox` | 需要可靠发送的 push/webhook/event，含 retry 与 delivery 状态 |
| `artifacts` | metadata、BlobRef、hash、provenance、retention 与 security label |

所有主键至少包含 `tenant_id`。大模型内容、图片和文件不直接无限写入 JSONB；超过阈值转到 S3-compatible blob store，并做 hash、MIME、大小和访问策略校验。

### 10.3 可以承诺的执行保证

| 范围 | 保证 |
|---|---|
| 数据库内 run/event/checkpoint 转换 | 单事务、幂等、fencing 后的一次提交 |
| 已提交 model response | 恢复时不再次向 provider 请求 |
| 只读 tool | 可配置安全重试 |
| 支持 idempotency key 的写 tool | 至少一次调用、一次业务效果取决于对端幂等实现 |
| 不支持幂等的外部写操作 | 可能处于 `unknown`；必须人工确认、查询对端状态或运行补偿，不能盲目重试 |
| SSE/A2A push/webhook | 至少一次投递；consumer 以 event ID 去重 |

### 10.4 代码升级

- 每个 run 固定 `graph_id + graph_version + state_schema_version + provider/tool versions`；
- 新部署只接新 run，旧部署 drain 旧 run；
- 长期等待 run 通过显式 migration 升级，migration 本身可审计、可回滚、可测试；
- 不允许在相同 graph version 下修改节点语义后直接继续历史 run。

## 11. 协议实现细节

### 11.1 MCP

- 同时支持 client 与 server；本地工具和 graph/workflow 可暴露为 MCP tools；
- 以 2026-07-28 stateless core 为主线，每个请求携带自描述路由信息；
- tools 输入输出完整支持 JSON Schema 2020-12；
- transport、auth、capability 与 extension 分离；
- OAuth issuer/resource 绑定、token audience、redirect 与 metadata 必须严格校验；
- Tasks 作为独立 extension 映射，不混入内部 Run；
- 对旧客户端只提供明确版本的 compatibility profile，不围绕已弃用 HTTP+SSE、roots/sampling/logging 增加新核心依赖；
- CI 必须运行[官方 MCP conformance suite](https://github.com/modelcontextprotocol/conformance)，并保存版本化报告。

### 11.2 A2A

- 实现 Agent Card、signed/extended card、message、task、artifact、stream、subscription、cancel、push config；
- `/.well-known/agent-card.json`、`A2A-Version`、supported interfaces 顺序与认证声明符合规范；
- REST/HTTP+JSON 与 JSON-RPC 首先达到正式门槛；gRPC 在同一领域映射稳定后加入；
- push notification 使用 outbox、签名、重试、event ID 与 receiver 幂等；
- 所有 task 查询先校验 tenant/principal/scope，再访问数据；
- file URL、webhook、card URL 都通过 SSRF 防护与重定向复核；
- CI 必须运行[官方 A2A TCK](https://github.com/a2aproject/a2a-tck)，每个声称支持的 transport 都要通过 MUST 项。

### 11.3 内部事件模型

第一版应覆盖：

- `RunStarted` / `RunStateChanged` / `RunCompleted`；
- `NodeStarted` / `NodeProgress` / `NodeCompleted` / `NodeFailed`；
- `MessageStarted` / `MessageDelta` / `MessageCompleted`；
- `ToolCallRequested` / `ToolCallProgress` / `ToolCallCompleted`；
- `StateSnapshot` / `StateDelta`；
- `ArtifactAdded` / `ArtifactDelta`；
- `InterruptRequested` / `InterruptResolved`；
- `UsageRecorded` / `PolicyDecision` / `ErrorRaised`。

每个 event 有 `event_id`、run 内单调 `seq`、causation/correlation、timestamp、schema version 和 redaction metadata。客户端可用 `Last-Event-ID` 恢复，无需重新开始 run。

## 12. Memory 与检索边界

必须分开三种状态：

1. **Graph state/checkpoint**：保证当前 run 正确恢复；
2. **Conversation/thread memory**：同一会话的短期上下文；
3. **Long-term knowledge/memory**：跨 thread 检索，带 provenance、ACL、TTL 和更新策略。

M0 范围决策已经将内建 RAG ingestion、vector database adapter、chunking 和 connector catalog 排除在 v1 之外。v1 不预先创建 `DocumentStore`、`Embedder`、`Retriever`、`Reranker` 或 `MemoryStore` 公共 trait；检索与长期知识通过普通 local tool 或 MCP tool 接入，并遵循相同的 schema、policy、provenance、budget 和审计规则。只有生产场景证明工具边界不足时，才通过新 RFC 设计最小的长期 memory/RAG API。

任何被检索内容都视为不可信数据：带来源与信任标签进入 prompt，不得提升为 system/developer instruction。外部长期 memory 写入需经过工具 policy，敏感结论可要求审批；调用证据必须支持过期、撤销和污染追踪。

## 13. 安全架构

安全基线采用 [OWASP Top 10 for Agentic Applications 2026](https://genai.owasp.org/resource/owasp-top-10-for-agentic-applications-for-2026/) 与 [NIST AI RMF GenAI Profile](https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-generative-artificial-intelligence)；不是发布前才补一轮扫描。

| 风险 | 强制控制 |
|---|---|
| Agent Goal Hijacking | 指令层级与来源标签、外部内容 taint、关键目标不可由普通数据覆盖、对抗 eval |
| Tool Misuse | 最小权限、参数 schema/业务校验、风险分类、allowlist、审批、预算 |
| Identity & Privilege Abuse | end-to-end principal/delegation、scope 收窄、短期凭证、tenant 隔离 |
| Agentic Supply Chain | lockfile、SBOM、签名/摘要、依赖审计、skills/MCP/A2A 来源策略 |
| Unexpected Code Execution | 独立 executor/sandbox 进程、无默认 shell、网络/文件/secret capability 显式授权 |
| Memory & Context Poisoning | provenance、ACL、信任标签、TTL、写入审批、撤销和污染审计 |
| Insecure Inter-Agent Communication | TLS、认证、card/signature 校验、schema、replay protection、allowlist |
| Cascading Failures | depth/fanout/concurrency/cost/time budgets、circuit breaker、kill switch |
| Human-Agent Trust Exploitation | 审批界面展示准确动作、目标、参数、风险与来源；禁止模糊确认 |
| Rogue Agents | 不可被模型修改的 policy boundary、运行时终止权、不可变审计、行为检测 |

额外要求：

- URL 获取统一经过 egress policy：DNS 解析后校验、阻止 loopback/link-local/private ranges、限制重定向、响应大小、MIME 与超时，防 SSRF/DNS rebinding；
- secret 只以 opaque handle 进入运行时，按调用即时解析，永不进入 prompt、checkpoint 或普通日志；
- 工具执行器默认进程隔离；高风险代码执行使用容器/microVM adapter，CPU/内存/磁盘/网络/系统调用均有限额；
- approval token 绑定 run、具体 action hash、principal、scope、expiry 与 nonce，不能跨动作复用；
- Agent Card、skill、tool spec、prompt template 和 policy 都需版本与 provenance。

## 14. 可观测性与评测

### 14.1 Telemetry

采用 W3C Trace Context 与 OpenTelemetry。GenAI semantic conventions 仍在发展，因此框架保存自己的稳定语义版本，再映射到 [OpenTelemetry GenAI conventions](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/README.md)。

每个 run 至少记录：

- graph/node/tool/model span 与 causality；
- queue、model、tool、checkpoint、approval 各阶段 latency；
- input/output/cache/reasoning token 与已知费用；
- retry、rate limit、timeout、cancel、policy deny、schema repair；
- model/provider/prompt/tool/graph/policy 版本 fingerprint；
- 内容日志开关和 redaction 结果。

### 14.2 Eval

StateKnot 的 eval 模块必须支持：

- versioned dataset 与不可变 test case；
- exact/schema/rule/custom/LLM-judge evaluator；
- trajectory/tool-selection/policy/cost/latency 指标；
- fake model 的确定性单元测试；
- provider cassette 的契约回归，敏感数据清理后才能入库；
- golden trace、A/B、baseline comparison 和置信区间；
- prompt injection、memory poisoning、tool misuse 与跨 Agent spoofing 的安全回归集。

LLM judge 不能作为唯一发布门禁；关键业务结果必须有确定性断言或人工标注基线。

## 15. Server 与部署形态

### 15.1 对外 API

建议稳定资源 API：

```text
POST   /v1/runs
GET    /v1/runs/{run_id}
POST   /v1/runs/{run_id}:cancel
POST   /v1/runs/{run_id}:resume
GET    /v1/runs/{run_id}/events
GET    /health/live
GET    /health/ready
```

协议端点由各 adapter 按规范暴露，不强行套进 `/v1/runs`。API 使用幂等键、明确 deadline、分页、ETag/版本和 RFC 9457 风格问题详情；SSE 支持断线续传和 bounded buffer。

### 15.2 进程角色

同一 binary 支持受控角色：

- `api`：认证、提交、查询、流式与协议入口；
- `worker`：抢占 runnable run/node，执行并提交结果；
- `scheduler`：处理 sleep、retry、lease recovery、outbox；
- `all-in-one`：单节点开发或低规模生产，但仍使用正式 PostgreSQL 语义。

生产环境支持多副本、graceful drain、readiness、连接池隔离、数据库迁移锁、备份恢复演练和滚动升级。单进程内存模式不得出现在生产部署示例中。

## 16. 依赖与兼容策略

- 公共核心类型尽量依赖 `serde`、`schemars`、`futures-core`、`thiserror` 等小而稳定的库；
- Tokio 是默认 runtime，但不把 `tokio::` 类型暴露到最核心 trait；
- HTTP 使用 Rustls 默认，禁止无验证 TLS；
- provider/protocol/数据库依赖按 feature 隔离，CI 覆盖 minimal/default/all-features；
- workspace 精确管理依赖版本并提交 `Cargo.lock`；
- 使用 `cargo deny`、`cargo audit`，关键依赖可引入 `cargo vet` 审核记录；
- public API 通过 `cargo-semver-checks`，协议 fixture 与数据库 schema 单独版本化；
- 每个正式 feature 有最小支持版本、兼容矩阵、弃用周期和迁移指南；
- 官方 SDK 发生破坏性升级时只影响 adapter crate，不能迫使核心用户立即迁移。

### 16.1 Apache-2.0 开源策略

- 根目录保存 Apache License 2.0 官方全文 `LICENSE`，并维护只包含必要归属信息的 `NOTICE`；
- 所有自有 crate 的 `Cargo.toml` 使用 `license = "Apache-2.0"`，同时填写 repository、documentation、readme、rust-version 与 categories；
- 自有源代码、文档和配置文件使用简短的 copyright + SPDX header；自动生成文件在生成器与产物清单中声明，不重复堆叠无意义 header；
- 贡献默认依 Apache License 2.0 第 5 节进入项目。首期建议使用 DCO sign-off；只有在基金会、公司法务或专利治理确有要求时再引入 CLA；
- `cargo-deny` 设置第三方许可证 allowlist，Apache-2.0/MIT/BSD/ISC 等宽松许可证可自动通过，copyleft、source-available、未知或自定义许可证必须人工评审；
- crates.io 包、源码归档、Docker/OCI 镜像和其他二进制发布物都必须携带适用于其实际内容的 LICENSE、NOTICE、第三方归属、SBOM 与 provenance；
- 项目名称、Logo 与商标另行治理。Apache License 2.0 不授予商标权，也不表示项目由 Apache Software Foundation 托管或背书；
- 开源仓库在首次公开发布前必须具备 `README.md`、`CONTRIBUTING.md`、`CODE_OF_CONDUCT.md`、`SECURITY.md`、`GOVERNANCE.md`、`CHANGELOG.md`、`NOTICE`、issue/PR 模板和发布签名说明。

## 17. 实施路线图

下面按“每阶段都满足其范围内的生产质量门槛”规划，不发布一次性过渡实现。

### 阶段 0：架构契约与纵向验证（2–3 周）

- RFC：领域模型、错误分类、graph 语义、durability guarantees、protocol mapping、安全边界；
- 打通一条真实纵向链路：HTTP/SSE → durable graph → model → MCP tool → checkpoint → approval interrupt → resume → A2A remote agent → artifact；
- PostgreSQL 故障注入验证：进程在 model、tool、node commit 和 barrier 前后崩溃；
- A2A/MCP 官方 TCK 在 CI 中跑通最小实现；
- 只有达到后续质量标准的代码才进入主干，验证失败的设计直接修改 RFC，而不是保留两套 API。

### 阶段 1：稳定核心与 Agent API（4–5 周）

- `stateknot-core`、`stateknot-runtime`、`stateknot-testkit`；
- 多模态 content、capability negotiation、structured output、streaming；
- OpenAI/Anthropic 第一方 adapter、local tool、policy middleware；
- fake model/tool/clock、provider cassette、单元与契约测试。

### 阶段 2：Typed graph 与调度器（5–6 周）

- graph compiler、parallel/join/reducer/route/loop/subgraph；
- 确定性 superstep、pending writes、cancel/deadline/retry/budget；
- property tests、并发压力测试与 model checker 覆盖关键状态机。

### 阶段 3：PostgreSQL 耐久运行时（5–6 周）

- journal/checkpoint/node attempts/tool ledger/interrupt/outbox；
- lease/fencing、多 worker、恢复与版本迁移；
- testcontainers、kill/restart、网络分区和数据库 failover 测试；
- blob store、retention、tenant isolation 与 RLS 可选配置。

### 阶段 4：协议正式支持（4–5 周）

- MCP client/server 及 conformance；
- A2A REST/JSON-RPC、card/task/artifact/stream/push 及 TCK；
- OAuth/TLS/SSRF/push outbox/签名；
- 保存 AG-UI 等后续协议的映射约束，v1 不创建对应 feature 或公共 API。

### 阶段 5：生产治理与发布候选（4–6 周）

- server/API、OTel、eval、安全回归、限流/熔断、admin/运维；
- 升级、备份恢复、灾难演练、性能与 soak test；
- API 稳定性审查、文档、示例、兼容矩阵、Apache-2.0 归属核验、SBOM 与发布签名；
- 至少两个不同业务场景的 production pilot。

人员估算：3–4 名资深 Rust/分布式系统工程师，加兼职安全、SRE 与评测支持，完整 v1 约 **24–32 个自然周**。单人实现并达到同等生产门槛，现实估算为 **12–18 个月**。如果先交付受限范围的首个生产版本，可以固定 provider、只做 A2A REST/JSON-RPC、暂缓 UI 协议，但不能降低持久化、安全和测试门槛。

## 18. 发布门禁

### 18.1 正确性与恢复

- 对所有持久状态机做 property/state-machine tests；
- 在每个持久化边界注入 crash，恢复后无已提交节点重复执行、无 event 丢失、无非法状态跃迁；
- 证明 fencing 能拒绝过期 worker 写入；
- reducer 在不同任务完成顺序下结果一致；
- 长时间运行 soak test 无任务泄漏、连接泄漏或无界内存增长。

### 18.2 协议

- A2A：每个正式声明的 transport 通过官方 TCK 的 MUST 项；
- MCP：client/server 通过官方 conformance core profile；
- 保存报告、规范版本、SDK 版本与例外清单；有例外就不能标记为完整支持；
- 对未知字段、乱序/重复事件、断流恢复、超大 payload 和恶意 URL 做 fuzz/negative tests。

### 18.3 工程与供应链

- MSRV/default/all-features、Linux/macOS、PostgreSQL 支持矩阵 CI；
- `fmt`、Clippy `-D warnings`、docs、semver checks、deny/audit/vet；
- 协议 parser、URL/security boundary 和状态机 fuzzing；
- release artifact 可复现、签名、SBOM、provenance；
- `cargo package`、源码归档和 OCI 镜像均验证包含正确的 LICENSE、NOTICE 与第三方归属；
- 无未解释的 `unsafe`，无明文 secret fixture，默认日志通过敏感数据检查。

### 18.4 性能与 SLO

在选定参考硬件与数据库规格后冻结 SLO，而不是给无上下文的营销数字。至少测量：

- runnable node 吞吐与 scheduler p50/p95/p99 延迟；
- 事件写入/订阅吞吐、SSE 慢消费者与重连；
- checkpoint 大小、写放大、恢复时间；
- 并发 active/suspended runs；
- provider 限流、tool timeout、数据库 failover 下的退化行为；
- 每 run 的数据库、存储、token 与网络成本。

## 19. 主要风险与处理

| 风险 | 影响 | 处理 |
|---|---|---|
| 前沿协议快速变更 | 公共 API 被 wire spec 拖着变化 | adapter 隔离、版本协商、fixture/TCK、正式/Beta 分级 |
| 框架范围失控 | 长期无法交付稳定版本 | 固定 v1 non-goals；provider/vector/UI 插件按门禁逐个加入 |
| 自有 durable runtime 复杂 | 并发与恢复 bug 风险高 | PostgreSQL 单一事实源、明确状态机、fencing、outbox、故障注入与形式化/模型测试 |
| Rust provider 生态仍在 1.0 前 | 上游破坏性升级 | 自有 trait、adapter pin、两家 provider 第一方实现；其他 bridge 延后 |
| 外部副作用无法 exactly-once | 重复支付/邮件/变更 | 风险分类、幂等键、调用账本、unknown 状态、审批和补偿 |
| prompt/skill/tool 供应链攻击 | 数据泄漏或越权执行 | provenance、签名、最小权限、sandbox、egress policy、安全 eval |
| 长 run 跨版本升级 | 无法恢复或语义漂移 | 固定 graph/version、deployment drain、显式 state migration |
| “通用抽象”丢失 provider 特性 | 性能和能力受限 | capability negotiation、extensions、第一方高保真 adapter |

## 20. 建议优先确认的产品决策

若没有额外业务约束，建议采用以下默认值继续实现：

1. 首个目标场景：企业内部工具型 Agent 与跨服务 Agent 协作，不先做开放互联网 Agent 市场；
2. 首批模型：OpenAI Responses/OpenAI-compatible + Anthropic；
3. 首批持久化：PostgreSQL 16+，S3-compatible artifact store；
4. 首批协议：MCP 2026-07-28、A2A 1.0 REST/JSON-RPC；
5. 部署：Linux container/Kubernetes 为正式基线，macOS 用于开发；
6. 许可证：项目明确采用 Apache License 2.0；如未来存在商业扩展，必须与开源核心保持清晰的代码、商标、发布物和依赖边界；
7. 公共 API 在真实纵向用例与故障恢复验证完成前保持 `0.x`，但运行保证与数据迁移从第一天按生产标准实现。

## 21. 当前 M0 交付物

仓库治理、CI、不发布的 facade crate、v1 scope baseline 与三个 Golden
Scenario 已经建立。下一步完成并评审四份 RFC：

1. `RFC-0001 Core Domain and Capability Model`（Draft）；
2. `RFC-0002 Graph Execution and Deterministic Reduction`；
3. `RFC-0003 PostgreSQL Journal, Checkpoint and Recovery`；
4. `RFC-0004 MCP/A2A Mapping, Identity and Security Boundaries`。

RFC 获得接受并由可编译 contract examples 验证后，再按第一条纵向链路实际需要把实验 crate 提升为受支持边界，并只创建已被证明必要的 `stateknot-runtime`、`stateknot-integrations`、`stateknot-server` 与 `stateknot-testkit`。当前未发布的 `stateknot-core` 用于验证 RFC-0001 的类型契约；RFC 评审期间的实现不得作为稳定 API 或协议支持发布。第一批代码必须落在最终持久化、安全和恢复边界上，而不是先写一个无法升级的内存 demo。

## 22. 主要一手资料

- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence) 与 [interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)
- [Pydantic AI](https://pydantic.dev/docs/ai/core-concepts/agent/)
- [Google ADK](https://google.github.io/adk-docs/)
- [Microsoft Agent Framework](https://learn.microsoft.com/en-us/agent-framework/)
- [AutoGen current repository/status](https://github.com/microsoft/autogen)
- [CrewAI Flows](https://docs.crewai.com/en/concepts/flows)
- [LlamaIndex](https://docs.llamaindex.ai/en/stable/)
- [Rig](https://rig.rs/) 与 [Swiftide](https://github.com/bosun-ai/swiftide)
- [A2A 1.0 specification](https://a2a-protocol.org/latest/specification/)、[official Rust SDK](https://github.com/a2aproject/a2a-rs)、[TCK](https://github.com/a2aproject/a2a-tck)
- [MCP 2026-07-28 release](https://blog.modelcontextprotocol.io/posts/2026-07-28/)、[official Rust SDK](https://github.com/modelcontextprotocol/rust-sdk)、[conformance suite](https://github.com/modelcontextprotocol/conformance)
- [AG-UI](https://github.com/ag-ui-protocol/ag-ui)、[MCP Apps](https://modelcontextprotocol.io/extensions/apps/overview)、[A2UI](https://github.com/a2ui-project/a2ui/blob/main/specification/v1_0/docs/a2ui_protocol.md)
- [AGNTCY](https://github.com/agntcy)、[SLIM](https://github.com/agntcy/slim)、[AP2](https://ap2-protocol.org/ap2/specification/)
- [Restate durable agents](https://docs.restate.dev/ai/patterns/durable-agents) 与 [Restate Rust SDK](https://github.com/restatedev/sdk-rust)
- [OpenTelemetry GenAI semantic conventions](https://github.com/open-telemetry/semantic-conventions-genai)
- [OWASP Top 10 for Agentic Applications 2026](https://genai.owasp.org/resource/owasp-top-10-for-agentic-applications-for-2026/)
- [NIST AI RMF Generative AI Profile](https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-generative-artificial-intelligence)
- [Apache License 2.0 official text](https://www.apache.org/licenses/LICENSE-2.0.txt) 与 [ASF applying guidance](https://www.apache.org/legal/apply-license)
