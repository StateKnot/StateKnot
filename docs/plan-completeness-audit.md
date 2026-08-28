<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 方案完整性与 Ponytail 审计

> 审计日期：2026-08-28
> 审计范围：当前仓库全部文件；架构主文档为 `docs/research-and-implementation-plan.md`。
> 方法：Ponytail 只审查过度设计；完整性、正确性、安全、运维和开源治理另做正常评审。

> M0 处理进度：[`v1-scope.md`](v1-scope.md) 已解决 v1 范围与 RAG 边界，
> [`scenarios/`](scenarios/README.md) 已固定三个场景、参考环境、负载、故障模型与首组性能/恢复阈值。
> Scheduler、storage lifecycle、schema migration、authentication/policy 和公共 API 仍必须由 RFC-0001 至 RFC-0004 关闭，不能因场景文档完成而视为已解决。

## 1. 总体结论

方案的架构方向是成立的，已经覆盖 Agent loop、typed graph、耐久执行、MCP/A2A、安全、可观测性、评测与部署，明显超过概念性方案。

但它目前还不是一份可以直接宣布“生产级 v1 范围已冻结”的完整规格，原因有两类：

1. **范围偏宽**：过早规划了过多 crate、运行时 adapter、UI/发现/支付协议、第二向量库、time travel/fork 和 microVM 等能力；
2. **验收缺口**：参考业务负载、API 易用性、调度公平性、数据生命周期、认证/策略实现、schema 迁移、RPO/RTO 和开源治理尚未形成可执行验收条款。

建议先收窄 v1，再补齐 P0 决策。两者必须同时进行：只删能力会留下生产缺口，只补能力会继续扩大范围。

## 2. Ponytail 整库审计

yagni: 首期规划 13 个 crate，边界在没有真实依赖压力和独立版本需求前过早固化。先合并为 `stateknot-core`、`stateknot-runtime`、`stateknot-integrations`、`stateknot-server`、`stateknot-testkit` 和 facade `stateknot`，出现明确编译、依赖或 semver 边界后再拆。[docs/research-and-implementation-plan.md]

yagni: 在只有 PostgreSQL 一个正式运行时的情况下预先设计 Restate/Temporal runtime SPI。先实现 PostgreSQL 语义；第二个后端真正立项时再抽取最小公共接口。[docs/research-and-implementation-plan.md]

delete: v1 路线中的 AG-UI Beta 交付，以及容易被理解为近期实现承诺的 MCP Apps、Agent Skills、A2UI、AGNTCY/SLIM、AP2。只保留协议观察表，不创建代码、feature 或稳定 API。[docs/research-and-implementation-plan.md]

yagni: v1 同时承诺 pause/resume、time travel 和 fork。保留生产必需的 pause/resume；等调试与审计场景有真实需求后再实现 time travel/fork。[docs/research-and-implementation-plan.md]

yagni: RAG 首期同时规划 pgvector 和另一个独立向量数据库。只交付 pgvector，真实用户需要独立向量库时再增加 adapter。[docs/research-and-implementation-plan.md]

yagni: 两个第一方 provider 之外再增加 Rig bridge，会同时承担三套能力映射和上游版本波动。v1 只保留 OpenAI-compatible 与 Anthropic。[docs/research-and-implementation-plan.md]

native: 自建容器/microVM 代码执行能力超出 Agent 框架边界。需要代码执行时调用现成 OCI/Kubernetes sandbox service，并只定义远程工具契约。[docs/research-and-implementation-plan.md]

shrink: 正确性、安全和发布要求在“生产级基线”“安全架构”“路线图”“发布门禁”中重复。保留一份规范性 release checklist，其他章节只引用它。[docs/research-and-implementation-plan.md]

delete: 仓库中的 `.DS_Store` 与项目无关。删除并在初始化 Git 时加入 `.gitignore`。[.DS_Store]

net: -90 lines, -0 deps possible.

## 3. 正常完整性审查：编码前必须补齐

### P0：阻止范围冻结

| 缺口 | 为什么重要 | 完成标准 |
|---|---|---|
| 参考用例与负载模型 | 当前先有架构、后有业务验收，无法判断哪些能力真是 v1 必需 | 固定 3 个 golden scenarios：工具型 Agent、长任务审批恢复、跨组织 A2A；为每个场景写请求量、并发、run 时长、artifact 大小、故障模型与成功标准 |
| 用户 API 与开发体验 | trait 片段不足以验证“框架是否好用” | 给出可编译的 first agent、typed graph、MCP tool、A2A server 四个端到端示例；限定常见场景的样板代码、错误可诊断性和文档路径 |
| Scheduler 与多租户公平性 | 只有 lease/fencing，不足以防止大租户饿死其他租户 | 定义 admission control、tenant queue、priority、weighted fairness、worker capability、quota、starvation bound 与 overload 行为 |
| 持久数据生命周期 | append-only journal 会无限增长，删除、审计和恢复要求可能冲突 | 定义分区、归档、compaction、checkpoint GC、artifact GC、租户删除、legal hold、备份/PITR、RPO/RTO 与恢复演练 |
| 序列化与迁移契约 | graph/version 字段存在，但没有规范 byte/wire compatibility | 固定 canonical encoding、ID/时间格式、未知字段策略、schema registry、N-1/N-2 migration test、降级策略和失败 run 处置 |
| Server 认证与 policy engine | 方案列出 scopes/OAuth，却未确定本地 API 的验证与授权实现 | 固定 OIDC/JWT/JWKS、mTLS 边界、key rotation、fail-open/closed、policy decision 接口，以及内置规则或 Cedar/OPA adapter 的选择 |
| RAG 的 v1 边界 | 文档声称支持 RAG，但只定义 retriever trait，没有生产数据生命周期 | 二选一：从 v1 移除 RAG；或补齐 ingestion、去重、chunk、embedding version、reindex、ACL、删除传播和 retrieval eval，并只支持 pgvector |
| 发布兼容与支持政策 | semver 工具存在，但没有用户可依赖的支持承诺 | 明确 0.x 与 1.x API 政策、MSRV 提升窗口、协议版本支持周期、数据库迁移支持范围、漏洞响应时限和 release cadence |
| 性能验收值 | 方案只列测量项，没有通过/失败标准 | 在阶段 0 固定参考硬件与至少一组 scheduler、event、recovery、active/suspended run 的 p95/p99 和容量门槛 |

### P1：首个公开版本前补齐

- 项目与 crate 命名已确定为 StateKnot / `stateknot`；首个对外版本前完成正式商标检索和商标政策；
- 建立贡献与治理：DCO、maintainer/committer 规则、RFC 流程、Code of Conduct、安全披露、支持边界；
- 定义 provider routing、fallback、cache、数据驻留和凭据轮换策略；
- 为 source archive、crates.io、Docker/OCI 和 Helm 分别维护内容清单、LICENSE/NOTICE、SBOM 与签名验证；
- 加入跨版本数据库迁移、跨语言 A2A/MCP interop、nightly live-provider 和灾难恢复 CI；
- 定义文档体系：README quickstart、mdBook、docs.rs、运维手册、迁移指南和协议兼容矩阵。

## 4. 建议的精简 v1

### 保留并做深

- typed content/model/tool API 与 structured output；
- OpenAI-compatible、Anthropic 两个第一方 provider；
- sequential、conditional、parallel/join、loop、subgraph 与 pause/resume；
- PostgreSQL journal/checkpoint、pending writes、lease/fencing、outbox 与多 worker；
- MCP 2026-07-28 client/server、A2A 1.0 REST/JSON-RPC client/server；
- REST/SSE、OIDC/policy、OpenTelemetry、最小 eval/testkit、安全与发布门禁；
- pgvector 仅在选择“RAG 属于 v1”后加入。

### 延后但保持兼容方向

- time travel/fork；
- Rig bridge 与第三个 provider；
- 第二向量数据库；
- AG-UI、MCP Apps、Agent Skills、A2UI；
- AGNTCY directory/identity、SLIM transport、AP2；
- Restate/Temporal runtime adapter；
- 自建代码执行 sandbox、microVM 与插件市场。

延后的能力只允许保留 ADR 中的兼容约束，不能提前创建空 trait、feature、crate 或配置项。

## 5. Apache-2.0 决策

项目已经明确采用 Apache License 2.0：

- 根目录已加入官方完整文本 `LICENSE`；
- 根目录已加入最小 `NOTICE`；
- 架构文档已加入 `SPDX-License-Identifier: Apache-2.0` 并将许可证从建议项改为正式决策；
- 方案已要求所有 crate metadata、源码/文档 header、第三方许可证审计和发布物归属检查统一执行；
- 使用 Apache-2.0 不表示项目与 Apache Software Foundation 存在隶属或背书关系。

项目名与 NOTICE 显示名已统一为 StateKnot，归属写作 `StateKnot contributors`。首个对外版本前仍需完成正式商标检索。

## 6. 下一步门禁

可以初始化仓库治理、CI 和不发布的 facade crate，但暂不按原 13-crate 结构铺开产品 API 与运行时实现。先完成以下六项并通过一次架构评审：

1. 冻结 3 个 golden scenarios；
2. 采纳或调整“精简 v1”边界；
3. 决定 RAG 是否属于 v1；
4. 决定 policy engine 与 server authentication 基线；
5. 完成 scheduler、storage lifecycle、schema migration 三份补充 RFC；
6. 冻结公开 API 示例和阶段 0 性能/恢复验收值。

完成后再初始化 workspace，第一条纵向链路就可以直接按最终生产边界实现。
