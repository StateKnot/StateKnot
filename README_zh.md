<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot

[English](README.md) | **简体中文**

[![CI](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml)
[![Supply chain](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**面向 Rust 的持久化 Agent 编排框架。**

[官方网站](https://stknot.com) · [中文文档](https://stknot.com/zh/docs/) ·
[English documentation](https://stknot.com/docs/)

StateKnot 是一个正在开发的开源 Rust 框架，用于构建类型安全、可持久化、
可观测且协议原生的 Agent 系统。它以 Rust 原生运行时为目标，而不是对某个
Python Agent 框架进行逐行移植。

> [!IMPORTANT]
> StateKnot 当前处于 **pre-alpha** 阶段。仓库已包含经过评审的架构基线、
> 项目基础设施以及持续扩展的纵向实现，但尚未发布生产版本或稳定公共 API。
> 现阶段请勿用于生产环境。

## 设计方向

StateKnot 围绕以下五项承诺设计：

- **默认类型安全：** 使用明确的状态、Tool Schema、结构化输出和能力协商，
  避免让无类型 Map 贯穿整个运行时。
- **持久化执行：** 提供事件日志、检查点、暂停与恢复、租约与 fencing、
  事务 Outbox，并明确外部副作用能够达到的真实语义。
- **兼顾 Graph 与 Agent 易用性：** 常见场景使用直接的 Agent Loop；分支、
  并行、汇合、循环和人工审批使用确定性的类型化 Graph。
- **协议原生互操作：** 将 MCP 和 A2A 作为一等协议适配层，同时避免其线协议
  类型泄漏到稳定的核心领域模型中。
- **生产治理内建：** 租户隔离、策略执行、预算、审计、OpenTelemetry、评测、
  故障注入和兼容性测试都是设计要求，而不是后期附加功能。

v1 范围基线包括 PostgreSQL 持久化执行、OpenAI 兼容模型与 Anthropic 模型
适配器、MCP Client/Server，以及 A2A REST/JSON-RPC Client/Server。具体支持面
和明确排除项记录在 [v1 范围基线](docs/v1-scope.md) 中。

## 当前里程碑

项目目前处于**架构契约与持久化运行时纵向验证阶段**。以下能力已经进入仓库并
由相应测试或资格门禁覆盖，但这不等同于稳定版或生产可用声明。

### Host、运维与资格验证

- [同进程 Agent Host](docs/agent-host.zh-CN.md) 统一管理认证入口、执行和维护，
  具备有序启动、同级就绪门禁和可等待的完整关闭流程。
- [只读运维监听器](docs/agent-operations.zh-CN.md) 采用独立授权，在业务故障时
  仅提供有界、脱敏的健康信息，并要求显式检查权限和带有效期的运维策略。
- [Host 资格验证工具](docs/host-qualification.zh-CN.md) 能从真实 PostgreSQL
  16/17 路径生成规范且有界的容量、恢复和故障证据。CI 中的缩减配置不作为
  发布证据；参考部署、负载、浸泡、故障转移和稳定版验收仍是独立门禁。
- 官网不部署测试身份系统或 Agent 运行时。

### 持久化核心与 Graph Runtime

- 未发布的核心 crate 已覆盖模型、Tool、Agent 准入与结果、运行生命周期、
  规范事件信封、Graph 检查点、Tool/模型调用状态机、不可变待提交节点结果、
  物理节点尝试恢复、固定 fence 的至少一次 Outbox，以及带完整性绑定的中断
  和定时器记录。
- PostgreSQL 16/17 持久化链路实现了运行准入、规范事件追加与读取、带锁状态
  转换、投影幂等、不可变 superstep 检查点、精确父子关系、反向谱系验证、
  哈希链接的 Tool/模型调用账本、运行级物理尝试注册表、数据库时间重试门禁、
  高 fence 故障接管、租约 fencing 和事务 Outbox。
- 调度发现采用租户隔离的可运行投影和数据库时间固定的 keyset 分页；内存解码
  有硬上限，无需逐运行轮询。Outbox 具备不可变目标快照、先持久化后派发、
  至少一次重试、死信与过期恢复。
- 受信控制面可以在疑似损坏的事件日志之外提交结构化运行隔离记录。隔离操作
  绑定稳定 ID、封闭原因、非敏感组件、证据校验和、精确日志观察以及当前租约
  fence，过期 Worker 无法停止后继 Worker。
- `ClaimedRunRecovery` 在稳定的检查点和日志观察上规划可执行节点，将激活项按
  `NodeId` 稳定排序并分类为已完成、可派发、延期、同 fence 执行中、终态失败
  或达到硬上限。每个激活最多允许 64 次物理尝试。
- `start_recovered_node_attempt` 在执行节点代码前原子提交并复核物理启动。
  只有新的 `Committed` 结果授予启动权限；`Idempotent` 表示已有执行，必须由
  更高 fence 进行孤儿恢复。
- 延迟重试通过独立 `scheduler_not_before` 门禁实现，并在同一事务内复核检查点、
  日志、租约、生命周期和数据库时间；丢失确认能够精确收敛。
- 完整 ready-set barrier 会原子绑定并消费不可变结果集，同时提交事件、后继
  检查点、生命周期投影和运行 head。绕过证据直接写后继检查点或待提交结果会
  被拒绝。
- 核心能够将有界声明式 Graph 编译为规范 JSON 和精确 SHA-256 身份，在准入前
  拒绝非法拓扑，并从完整结果集确定性地产生 barrier 意图。不可变 Graph 注册表
  按租户保存编译定义；恢复时会重新编译并核对检查点固定的身份与摘要。
- 未发布的 `stateknot-runtime` crate 提供离线、摘要固定的 JSON Schema 2020-12
  注册表，精确的 Graph/Reducer/Node 可执行绑定，所有非初始已提交检查点的独立
  有界重放，以及带 fence 的持久化 Graph Driver。
- Driver 在启动节点代码前持久化物理尝试，不会从幂等观察到的启动重复派发；
  它在租约临近过期时先刷新所有权，使用基于数据库时间的单调过期看门狗续租，
  支持协作取消，并针对最新日志 head 提交成功或失败。
- 明确标记为受信 `JournalIsolated` 的同级节点可在有限进程/Graph 并发上限内
  重叠执行；启动与完成事实仍按 `NodeId` 规范排序，独占执行器保持单独运行。
- 生命周期协调器、有界持久化 Agent Loop 和租户级 Scheduler Worker 已接入同一
  运行时。公平调度使用不可变分片级平滑加权策略、全局有序且可恢复丢失确认的
  reservation、精确周期份额和显式饥饿上限。
- 静态共享状态子图与有界循环编译进同一持久化 Graph，保留精确源版本、作用域
  节点/路由 ID、源顺序以及强制返回/耗尽 continuation。独立子状态和生命周期
  尚不属于该配置，详见 [Graph 组合指南](docs/graph-composition.md)。

### 模型、Tool 与 Agent Runtime

- 运行时固定模型和 Tool Provider 注册表，通过受信预算/期限准入、先持久化后
  派发、结果验证、对账安全失败以及不派发的终态恢复来执行持久化尝试。
- 一等 OpenAI Responses 和 Anthropic Messages 适配器支持 unary/SSE、传输边界
  控制，以及无损的 Provider 原生 unary Tool continuation。
- `DurableAgentAdmission` 原子提交不可变认证 Agent 意图、数据库时间准入、
  Active 生命周期、首条审计事件、superstep-zero 检查点和调度投影。精确重试
  恢复原始提交，任何输入、策略、Graph、状态或身份冲突都会失败关闭。
- `DurableAgentRuns` 提供租户级持久化入口键、新 ID 下的丢失确认收敛、内容变更
  冲突检测、完全复核的公共运行快照，以及成功、失败和取消终态结果。
- `ProviderNativeAgentGraph` 支持串行或有界并行的只读多轮模型/Tool 执行、写操作
  串行 barrier、Provider 原生 transcript 重建、摘要固定的本地策略、确定性记账、
  不重复派发恢复、已知 Tool/模型失败 continuation、有界结构化输出修复和两阶段
  取消确认。
- `AgentServiceV1` 提供精确版本、授权优先的嵌入边界，用于租户级提交恢复、运行
  与入口键读取以及由调用方保留身份的两阶段取消。
- 可选的 [Agent HTTP v1](docs/agent-http.md) 暴露认证、有界的 JSON 提交、读取、
  入口键查询和取消接口；[Activity SSE](docs/agent-events.md) 支持 PostgreSQL 游标
  重连、公共通知和独立观察快照。它不是 Token 流，也不提供完整历史状态重建。
- [HTTP Server](docs/agent-http-server.md) 管理 loopback 连接、真实依赖检查和有界
  优雅关闭；[Scheduling Worker](docs/agent-worker.zh-CN.md) 以固定并发管理租户与
  公平调度器，提供真实就绪状态、有限期限和包括嵌套 Graph 节点的完整关闭。
- [在线身份配置](docs/agent-identity.md) 支持固定 HTTPS OAuth introspection、
  精确 claim/scope 校验、轮换客户端密钥、带有效期且默认拒绝的租户绑定和负向
  canary 就绪检查。JWT/JWKS 不在当前实现范围。
- [声明式资源策略](docs/agent-resource-policy.md) 提供真实的默认拒绝授权器、精确
  调用方/Agent/Schema/Run selector、显式租户运维角色、收紧预算和确定性准入
  证据。策略不会推断运行所有权，也不替代受保护访问审计。

### MCP

- `McpRemoteTool` 是首个严格协议适配器，支持 MCP 2026-07-28 现代无状态发现、
  完整 JSON 响应、本地 Schema 与服务端身份固定、按尝试授权、有界传输和歧义
  写操作优先对账。它是 Client 侧 Remote Tool 配置，不代表完整 MCP Client/
  Server 一致性声明。
- 通用无状态 `McpClient` Tool 面支持有界发现与分页、JSON/请求级 SSE、标准和
  嵌套自定义 Header、逐请求授权、无效 Tool 隔离、禁止网络 Schema 解引用以及
  精确多轮请求状态。
- `McpOAuthAuthorization` 支持由 challenge 驱动的受保护资源与授权服务器发现、
  预注册/CIMD/DCR、PKCE、issuer 与 callback 校验、scope 升级、refresh、有界
  replay 和调用方自有持久化 Store。
- 固定版本的官方 Client Runner 覆盖全部 32 个计分场景（含全部 25 个 OAuth
  场景）：371 条断言通过、0 失败；11 项超出已声明 Tool 面的能力/方法检查明确
  跳过。另有 7 项不计分扩展仅报告、不声明支持。
- StateKnot 自有 MCP Server 应用层组合不可变且有界的 Tools、Resources、
  Resource Templates、Prompts 和可选 Completion，并置于生产级无状态 HTTP
  边界之后。它执行认证、准入、披露前授权、精确 scope、离线 JSON Schema
  2020-12 校验、主体绑定分页、输出校验、进度、取消和多轮请求状态完整性绑定。
- 官方 Server 门禁覆盖全部 37 个计分场景：114 条断言通过、5 项能力检查跳过、
  1 项 SSE 检查仅作信息展示，0 失败或警告；另有 3 个不计分 Schema/Header
  门禁提供 32 条通过结果。
- Final SEP-2640 MCP Skills 扩展具有独立 Server 与 Client/Host 配置。Server
  针对精确服务字节固定完整 manifest，并在披露前授权；Host 必须显式启用、验证
  完整有界 manifest、独立指定来源、惰性读取前取得新批准，并核对精确大小和
  SHA-256。
- Skill Host 隔离不可变内存缓存，嵌套 Skill 要求新的同意，并在 PostgreSQL
  schema 26 中保存精确的运行级激活批准和数据库时间生效窗口。调用方保留的
  重试 ID、重启恢复、父级锁、不可变撤销和活动窗口串行化均在 Provider I/O 前
  失败关闭。
- `McpSkillBoundTool` 可用已激活 Skill 保护精确注册的 Tool，绑定完整描述符，
  要求独立执行/对账批准，并继续使用普通持久化 Tool 尝试账本，不创建第二套
  Dispatcher。
- 已授权的[结果对账 Tool](docs/mcp-reconciliation.zh-CN.md)可在不重新执行写操作的
  前提下，使用权威证据将精确 Unknown 尝试与结果原子保存；[已知错误对账 Tool](docs/mcp-error-reconciliation.zh-CN.md)
  只在存在权威“已应用/未应用”证据时将 Unknown 转为 Failed。
- [输入感知的 MCP 写绑定](docs/mcp-call-authorization.zh-CN.md)固定完整原始 Tool
  描述符并按持久化尝试授权精确参数；中断连接会在凭据复用前退役。不确定写操作
  仍需要权威对账，这不构成外部执行 exactly-once 保证。
- 受限 [MCP Compute Worker](docs/mcp-compute-worker.zh-CN.md)可在没有数据库凭据的
  情况下委派纯计算；受信 Driver 保留全部持久化权限。它不是任意代码沙箱或通用
  有副作用 Worker API。
- MCP Tasks、动态 Skill manifest、磁盘安装、签名、自动发现到 Agent 组合、
  更广泛 Client 扩展、稳定 API/SDK 等级声明和完整生产资格验证尚未实现或声明。

### A2A 与 Artifact

- A2A 1.0 Server 将官方 SDK 线协议类型封装在 StateKnot 自有的有界 Agent Card、
  Message、Task、Artifact、Stream 和 Push 契约之后。HTTP+JSON 与 JSON-RPC/SSE
  边界执行精确 Host/Origin/Route/Version/Extension 策略、解析 Body 前认证、
  查询 Task/配置前授权、调用方自有副本准入、有界响应和优雅关闭。
- 固定校验和的官方 Server TCK 共 265 个用例：177 通过、88 明确跳过，0 失败、
  error 或 xfail；关键 Streaming、多订阅者、认证 Push、扩展 Card、缓存、错误
  映射和未知字段用例必须执行。
- 严格 A2A 1.0 Client 实现全部 11 个 HTTP+JSON 与 JSON-RPC 操作以及两种 SSE
  Surface。发现过程固定有界 Agent Card、服务端首选接口、精确 egress、扩展、
  租户和安全选项。
- 当前认证配置支持完整的单一方案 HTTP Bearer、OAuth 2.0 或 OpenID Connect；
  API Key、Basic、mTLS 和多方案认证仍是明确排除项。
- `A2aRemoteAgent` 将一个已公布 Skill 与本地输入/输出 Schema 绑定到现有持久化
  Tool 契约，可选择 AtMostOnce 或由运维方证明的消息 ID 去重语义。真实 loopback
  测试覆盖两种绑定的完整操作矩阵；真实 PostgreSQL 测试证明先落库后发送、丢失
  响应进入 `Unknown`，并且重复执行不会再次派发。
- 可选的运维证明恢复可以查询有界 Context/Task 历史且不重发，或仅在持久化
  去重证据存在时重放精确消息 ID。Provider 原生 Agent Turn 会把 `Pending` 转为
  持久化延迟重试，并在不重复业务调用的情况下提交权威证据。
- Artifact 路径可以接收返回的 Task Handle，直接轮询与精确 Endpoint 绑定的 Task，
  不重发业务消息，并在 Tool JSON 之外物化终态 Part。
- `stateknot-artifact-store` 通过 staging 和有条件最终创建将字节发布到私有 S3
  兼容后端；启动时探测后端契约，注册表读取前先授权，并在注册前和每次读取时
  核验完整长度与 SHA-256。迁移 18 保存不可变租户引用、来源事件、谱系和私有
  Object Locator。
- A2A 官方 Client 一致性、真实 Peer 恢复资格验证、gRPC 以及生产级持久化 Server
  Task/Push Backend 仍是独立门禁。

### 当前开发目标

1. 将已完成的 PostgreSQL 严格 MCP 恢复证明持续作为 PostgreSQL 16/17 强制门禁；
2. 保持 MCP Client/Server 两套独立配置及固定的 32/37 项官方门禁，完成稳定 API
   评审，并让 Tasks 和其他扩展维持独立声明；
3. 保持 A2A 1.0 HTTP+JSON/JSON-RPC Server 门禁和已实现的持久化 Client、出站及
   对账边界，为精确部署证明补充官方与真实 Peer 资格验证；
4. 验证三个冻结的生产场景，验收核心领域、Graph、持久化和协议/安全 RFC；
5. 完成角色隔离、故障转移/恢复和最终陈旧竞态门禁；
6. 在声明支持前发布兼容性与性能证据。

进一步阅读：

- [资格验证场景](docs/scenarios/README.md)
- [路线图](docs/roadmap.md)
- [完整调研与实现计划](docs/research-and-implementation-plan.md)
- [PostgreSQL Provider 运维指南](docs/postgresql-provider.md)
- [公共核心契约示例](docs/core-contract-examples.zh-CN.md)
- [类型化 Agent 与一等适配器](docs/typed-agent.zh-CN.md)
- [持久化 Agent 准入](docs/durable-agent-admission.zh-CN.md)
- [持久化 Agent 运行与结果](docs/durable-agent-runs.zh-CN.md)
- [Provider 原生 Agent Graph](docs/provider-native-agent.zh-CN.md)
- [共享状态子图与有界循环](docs/graph-composition.zh-CN.md)
- [AgentService v1](docs/agent-service.zh-CN.md)
- [MCP 一致性状态](docs/mcp-conformance.zh-CN.md)
- [A2A 1.0 Client 与持久化 Remote Agent](docs/a2a-client.zh-CN.md)
- [持久化 Artifact 存储](docs/artifact-storage.zh-CN.md)
- [A2A 1.0 Server](docs/a2a-server.zh-CN.md)
- [A2A 一致性状态](docs/a2a-conformance.zh-CN.md)
- [持久化调用执行器](docs/durable-invocation-executor.zh-CN.md)
- [跨租户公平调度](docs/cross-tenant-fair-scheduler.zh-CN.md)
- [完整性审计](docs/plan-completeness-audit.md)

## 仓库结构

```text
crates/stateknot/        用于验证 workspace 的未发布门面 crate
crates/stateknot-core/   已验证的领域、运行、日志、检查点、调用和所有权契约
crates/stateknot-artifact-store/  私有对象发布、不可变 Artifact 注册和授权验证读取
crates/stateknot-integrations/  OpenAI/Anthropic 适配器及有界 MCP、A2A 协议配置
crates/stateknot-runtime/  AgentService v1、可执行/Provider 注册表、持久化 Driver、调用执行器、Agent Loop 和公平调度器
crates/stateknot-store-postgres/  PostgreSQL 日志/检查点/调用/租约/Outbox 持久化链路
docs/                    架构契约、资格验证场景和路线图
website/                 Astro 中英文文档站、浏览器测试和 Caddy 部署配置
.github/                 贡献模板和自动化质量门禁
```

只有在依赖边界或语义边界得到证明后才会增加新的 crate。项目会刻意避免创建没有
实际实现的 Provider 或协议空壳 crate。

## 本地开发

仓库固定使用 Rust 1.88.0，这是官方 MCP Rust SDK 3.x 协议适配器要求的最低
Rust 版本。安装 `rustup` 后，工具链会自动选择。

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
```

部分持久化、协议一致性和资格测试需要 Docker、PostgreSQL 或固定版本的外部测试
Runner。请以仓库脚本和相应文档中的门禁说明为准，不要把被跳过的集成测试视为
通过。

官网使用独立锁定的 Node.js 工具链和验证流程，详见
[网站开发指南](website/README.md)。提交实现前请先阅读
[CONTRIBUTING.md](CONTRIBUTING.md)。公共 API、持久化语义、协议、存储或安全边界
的变更必须先提交 RFC。

## 社区与安全

- 使用 GitHub Issue 提交可复现的 Bug 或范围明确的功能提案。
- 请遵守 [行为准则](CODE_OF_CONDUCT.md)。
- 安全问题请按照 [SECURITY.md](SECURITY.md) 中的私密流程报告。
- 项目决策流程记录在 [GOVERNANCE.md](GOVERNANCE.md) 中。

## 许可证

StateKnot 采用 [Apache License 2.0](LICENSE) 开源，贡献代码使用相同许可证并要求
Developer Certificate of Origin（DCO）签署。该许可证不授予项目名称或 Logo
的使用权。
