<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# R1 核心契约验收缺口清单

核对日期：2026-10-09。基线为 R0 JWT/JWKS 的已验证源码
`8c57db8cbff754428a2e876b98f7c2b6c3ef8edd`。
本清单逐条对应 RFC 的验收项，记录已有证据、仍需实现或验证的边界及负责阶段。
“已有证据”表示限定路径存在可执行验证，不表示对应完整条目或 RFC 已验收。
RFC-0001 至 RFC-0004 继续保持 Draft。
公开类型 fixture/property/fuzz 的具体补齐工作由
[#149](https://github.com/StateKnot/StateKnot/issues/149) 跟踪。

## RFC-0001：公开核心类型

编号对应 [RFC-0001 Validation and rollout](rfcs/0001-core-domain-and-capability-model.md#validation-and-rollout)。

| 编号 | 已有证据 | 仍需交付 | 阶段 |
| --- | --- | --- | --- |
| C1 | 四个公开契约示例、MSRV CI、`dependency_boundary` 检查；原 RFC 已记录本项完成 | 后续修改继续保留这三个门禁 | R1 回归 |
| C2 | 44 个 fixture 的封闭目录、内容摘要和 catalog root；38 个公开值类型逐型正/负规范往返；全部 18 个宏生成 UUIDv7 类型有闭合源码清单门禁；另有 67 个执行/持久化类型的 104 个完整正向 wire 映射、错误形状/重复键/未知字段及 32 个摘要字段替换/遗漏拒绝，八个原有构造族保留旧摘要与完整 wire 对照；Tool/Skill 授权证据等类型族已覆盖。570 个根导出项已有完整分类和编译器门禁：307 个 reader/writer 的规范 wire、308 个输入/default 及 308 个输出 schema pin、两个仅输出生产者和 246 个 Rust-only 代表实例；准入/子 Run/Join/续写完整载荷由六个原有构造族复现。RFC-0021 为全部 181 个封闭对象增加流式 map 门禁，文本/Value 拒绝完整、空、截短及扩展数组；嵌套 schema 和真实 Tool 零调用路径有回归，原 fixture/wire/双向 pin 保持一致。另有编译器核对的 71 枚举/298 替代分支、73 项规范 wire/digest 与错误形状/原始重复键检查；28 个原缺失分支由公开构造器补齐，43 份原文档保持精确字节。 | 当前根类型基准映射已补齐；继续变体组合与嵌套负向审查，复用 C3/C4/C6 的独立交付；文件数量不能代替覆盖率 | R1 |
| C3 | 中英 Core guide 已映射五类要求；累计 44 个属性测试，每个 256 个有界样例，固定种子 CI 和普通随机运行；budget、reservation、scope、extension 等既有 proptest 保留；嵌套 JSON 的六维独立会计、精确/减一/随机收窄、UTF-16 规范树及宽值进入扩展时的重新收紧均有模型；另有七个复合预算模型，以独立宽整数会计和币种 map 核对所有用量字段、峰值、限制交集、收窄、预留及剩余容量，容量样例包含成功路径，逐维溢出/未列明币种有确定性拒绝；两个 Graph/checkpoint 模型的独立规范字节与摘要核对见 G2 | 已补齐标量构造/精度、Unicode 规范顺序、三方委托交集、嵌套 JSON/扩展逐值资源及纯预算组合模型；持久化 child account 和 uncertain effect 的端到端组合仍须独立验证；其他复合类型及完整变体审查继续开放 | R1 |
| C4 | 三个固定 ASan/libFuzzer 入口覆盖严格有界 JSON/JCS、全部 307 个 reader、全部 298 个枚举替代分支和实际离线 schema 注册表；逐一重放固定语料后有限变异，保留未知字段、重复键、畸形 Unicode、深层/超大结构、可选输出及对象形状反例；输入 schema oracle 在实际 reader 接受后执行，输出 oracle 保留；源快照、锁文件、引擎、进程生命周期与资源有显式门禁，CI 保留失败和覆盖语料。见 [fuzz 指南](../fuzz/README.md)。 | [#160](https://github.com/StateKnot/StateKnot/pull/160) 最终源码通过全部 14 项 CI；本地与 CI 均重放 2,241 个种子并各目标执行 10,000 次变异，源快照及两份锁保持一致，合并 tree 与验收 tree 一致。本条有限资格门禁已补齐，后续新增反例继续回归；不替代穷尽覆盖、真实历史版本或 R7 独立安全评审 | R1 合并门禁；后续回归 |
| C5 | 18 个第一方临时 context/key/credential/carrier 的序列化拒绝及构造对照；typed Tool 输入/输出缺少 JsonSchema 的完整编译失败实现；真实离线注册表拒绝非法 schema、非对象输入、缺失/替换 pin 及变化后的 descriptor，拒绝路径检查零应用调用 | 本轮补齐当前列举类型与 typed adapter 的证据；方向性 schema 修正属于 RFC-0019 已接受的限定源码契约，可选生产者 schema 由 RFC-0020 接受限定源码契约，新增类型继续纳入同样门禁；自定义 Serde 正确性仍须审查，SDK OAuth store records 只允许受信加密存储 | R1 回归 |
| C6 | 当前格式的负向 fixture 和显式版本检查 | 支持窗口内 N-1/N-2 的真实历史 fixture、向前迁移、新版本拒绝与损坏拒绝；不能生成虚假的历史版本 | R1 契约；R6 升级 |
| C7 | `dependency_boundary` 检查限定 core 的直接依赖；core 无 Tokio/provider/数据库依赖 | 保留传递依赖审查和最终不可变版本的依赖政策证据 | R1；R7 最终复核 |
| C8 | 三个 GS 场景的规范文本、多个缩减 runtime/Host fixture | 逐值映射场景需要的身份、预算、消息、结果、恢复和审计值；完整场景运行另有门禁 | R1 映射；R6 运行 |
| C9 | 脱敏诊断、跨租户、descriptor 替换、授权和未知结果等分散安全测试 | 按原 RFC 七类威胁整理审查记录并补漏；最终独立安全评审属于 R7 | R1；R7 |

规范化/schema 的实现依赖选择仍需 GS-001 事件率下的测量，归 R6。
后续 C4 回归保留了一个嵌套时间戳反例，修复校验前求值导致的算术 panic；
新增确定性检查覆盖 20 个数字位置的全部 2,360 个 ASCII 非数字替换、保持
字节长度/分隔符的 Unicode、直接与嵌套 Serde。有效时间范围/wire/schema pin
保持一致，任何新失败都必须修复生产边界并在最终源码重新验收。
现有预览发布不构成稳定 API 承诺；不能以完成 C1 或目录检查接受整个 RFC。

## RFC-0002：确定性 Graph 与调度

编号按 [RFC-0002 的十一条验收项](rfcs/0002-deterministic-graph-and-scheduler.md#validation-and-rollout)排列。

| 编号 | 已有证据 | 仍需交付 | 阶段 |
| --- | --- | --- | --- |
| G1 | graph/checkpoint/barrier/node-attempt/node-result 的 schema 与 canonical fixture；新增完整 wire 对照与逐型 reader，覆盖节点四种 control、等待、终结和 model/tool 绑定 | 全部当前公开序列化类型已纳入 307-reader/308-schema 清单；保留新变体与完整性组合审查 | R1 |
| G2 | 既有 graph/barrier/recovery 顺序属性保留；新增两个独立模型，每个 256 个样例：随机节点插入/结果输入顺序，独立状态与 route 并集，后继 checkpoint 规范字节及状态/意图/记录三类摘要 preimage；一至十三代 Unicode 状态链核对父 head 和 journal anchor，并拒绝状态篡改 | 当前纯根 barrier/checkpoint 边界由 [#160](https://github.com/StateKnot/StateKnot/pull/160) 的完整本地及 14 项最终 CI 验收通过，合并 tree 与验收 tree 一致；相同已提交事实不等于不同 journal 历史，同 Run 嵌套及持久化故障另见 G3/G4 | R1 |
| G3 | 顺序、条件、并行、等待、取消、静态共享状态组合、有界循环的迭代/耗尽/等待恢复及多条真实数据库路径 | 已有有界循环的限定路径继续回归；同 Run 嵌套完整恢复由 [RFC-0022 草案](rfcs/0022-namespaced-graph-frames.md) 明确身份、独立 checkpoint、等待/父续接和复合事务 journal 绑定。[#161](https://github.com/StateKnot/StateKnot/pull/161) 持续开发中：六个实验性 Core 帧/调用/屏障 reader 加入封闭清单、双 schema pin、当前源码 fixture 及 fuzz oracle；九项确定性与两个各 256 样例的独立模型验证 scope、pin、七层真实激活及连续父链，调用声明进入编译图摘要，serial caller、固定 route、schema/owner、实际目标/深度/闭包字节及 executor 冲突有源级检查。scoped recovery 的四项检查验证作用域、同 fence 执行、接管及已提交结果复用；帧屏障另有五项检查和一个独立 256 样例归并/规范摘要模型，共享既有 schema/reducer/control 校验，旧根屏障继续拒绝非根结果。仅 `CompiledGraph` 与 `ChildRunAdmissionIntent` 的 schema profile 显式变化，旧根 wire/定义摘要保持一致；这不构成执行支持。调用/返回准备已有精确声明/目标/caller/base、隔离快照与 terminal 固定 route 校验，十一项构造器检查；迁移 27、显式根查询与 21 约束/8 列/7 索引的精确 scope catalog 已实现，PostgreSQL 16/17 已执行非空源码 schema 26→27 与 scope FK 检查。手工 scoped 行不证明准入/复合 journal 事务或历史 N-1/N-2；完整 driver、入栈/返回/等待事务及故障矩阵仍需实现，现有 flatten/root namespace 不替代嵌套执行 | R1 |
| G4 | 多组 commit-loss 与 post-commit OS-kill 专项 | pending result、journal、checkpoint、lifecycle、outbox 的每个写入边界完整矩阵及组合故障 | R1 矩阵；R6 完整验证 |
| G5 | ready-node 恢复、不可变成功结果复用、exact-key runtime/Agent 重放 | 将重试与 sibling 结果复用结合到所有新增组合路径，确保不重复已提交 model/tool 调用 | R1 |
| G6 | logical-result 冲突、compiled graph/schema pin、corruption quarantine | 新增 namespace 与组合路径保持同样的拒绝和隔离语义 | R1 |
| G7 | 限定次数的 stale-fence/race 测试 | 10,000 次强制租约竞态，旧 Worker 写入成功数为 0 | R6 |
| G8 | 持久化 wait 和索引化就绪发现 | 100,000 挂起任务，无逐任务常驻 task/polling，额外 scheduler 内存不超过规范阈值 | R6 |
| G9 | 加权调度实现、缩减 Host 测量工具 | 参考拓扑下公平性、饥饿界限、排队、恢复和过载报告 | R6 |
| G10 | 独立逻辑 restore 与物理 named-PITR drill | 同步 standby failover、完整参考数据集恢复和完整性检查 | R6 |
| G11 | populated migration、部分 checkpoint 恢复与拒绝测试 | 支持窗口内历史 graph/checkpoint fixture、升级中断和 rollback-limit 验证 | R1 契约；R6 升级 |

剩余语义决策：typed builder、reducer 注册/版本、非 root namespace grammar 与
shared-state declaration 属于 R1；参考负载下调度策略和阈值属于 R6。
blob checkpoint 若引入须另立 RFC，并先依赖 R2 的 Artifact 生命周期。

## RFC-0003：PostgreSQL 持久化

编号按 [RFC-0003 Before RFC acceptance](rfcs/0003-postgresql-durability-recovery-and-migration.md#validation-and-rollout)的十条排列。

| 编号 | 已有证据 | 仍需交付 | 阶段 |
| --- | --- | --- | --- |
| P1 | core canonical、strict Serde、bounded schema、redacted diagnostics 与部分 proptest | 完整类型映射和随机状态验证，复用 C2/C3/C4 的交付 | R1 |
| P2 | PostgreSQL 16/17 append/renewal/expiry/revocation/fencing 测试 | 10,000 forced stale-worker trials，接受的过期写入为 0 | R6 |
| P3 | 多组业务事务的 COMMIT 请求/响应丢失及 post-commit kill profile | 每个 insert/projection/head/commit/ack 的完整故障矩阵 | R1 矩阵；R6 完整验证 |
| P4 | 100 个同步 appender 的真实 PostgreSQL 16/17 专项已通过：固定 intent/projection、1,024 次重试上限、60 秒并发 join 期限、完整事件身份和前序摘要链、最终 head、真实 Pending → Active 投影、竞争中/后的原请求重放及 projection 替换拒绝；中英 PostgreSQL 配置 guide 提供强制复现命令 | 本项有界 Journal 并发条件已补齐，保留完整 CI 回归；48-connection fixture pool 及本机执行不能替代 R6 参考拓扑、fencing、failover 和 soak | R1 回归；R6 独立验收 |
| P5 | 同 ID lost-ack 重放和真实 lease takeover | primary failover 后同 ID 收敛以及同步复制确认边界 | R6 |
| P6 | payload/intent/event/predecessor/checkpoint/graph corruption 隔离测试 | 完整 blob 与跨存储恢复组合；新增执行路径继续先校验再派发 | R1；R6 |
| P7 | 从非空旧 schema 向前迁移、catalog 校验和新 schema 拒绝的专项 | 支持窗口内 N-1/N-2、backfill 中断及明确 rollback window | R1 契约；R6 升级 |
| P8 | journal/checkpoint 的完整性基础 | archive/compaction 的可信 boundary、法律保留、可续跑 GC 和清理测试 | R2 |
| P9 | named-PITR 的限定 child-Join 数据集恢复 | 参考数据集、运行状态校验及 PostgreSQL/对象存储一致恢复 | R6，依赖 R2 |
| P10 | `CiReduced` Host harness | 三场景完整延迟、内存、恢复和 24 小时曲线 | R6 |

已有 trusted-server SQL role profile 不允许把 runtime 数据库凭据交给不可信 Worker。
Worker-only SQL/service 权限边界仍需 R1 决策及 R5 部署验证。
索引、分区、checkpoint 频率和 timeout 默认值必须由已提交测量决定。

## RFC-0004：子 Run

编号对应 [RFC-0004 五层交付](rfcs/0004-durable-child-runs.md#validation-and-rollout)；
所有已实现层仍受其 acceptance blockers 约束。

| 编号 | 已有证据 | 仍需交付 | 阶段 |
| --- | --- | --- | --- |
| D1 | identity/declaration/accounting 的边界、摘要、篡改和 canonical 测试 | 完整公开类型映射，跨层 identity/authority/unknown usage 对照 | R1 |
| D2 | 原子 admission/ownership/reservation、lost-ack、stale fence、populated upgrade | 与剩余完整 profile 语义结合的资格验证；不能只测独立 API | R1 |
| D3 | terminal binding、Join/wakeup、cancel queue、failure close 和 settlement 多项 real-store 验证 | 任意 uncertain direct effect、parent direct + child usage、各 parent close 路径的组合矩阵 | R1 契约；R6 完整验证 |
| D4 | opt-in spawn/Join、registry recreation、独立 child lease/schema 和 executable examples | 新 namespace/ownership 和 uncertain effect 下的全流程恢复 | R1 |
| D5 | PostgreSQL 16/17 与多组源码 CI/中英教程 | 完整 profile 验收后才允许 capability enablement；真实角色部署和容量报告 | R5/R6；R7 发布 |

逐条保留五类 blockers：Worker-only 权限边界、完整 failure-close/provider-effect
资格验证、parent direct/unknown usage 会计、Join/deadline/takeover 组合进程故障、
测量后的恢复/容量阈值。现有 post-commit、COMMIT-loss、logical restore 和 named-PITR
profile 的限定证据继续有效，但不能合并成未经执行的完整矩阵结论。

## R1 其他必需交付

| 边界 | 当前证据/限制 | 下一步验收 |
| --- | --- | --- |
| 不确定外部副作用 | Tool 的 `Unknown + ReconcileFirst`、MCP 授权 result/known-error reconciliation 和 A2A exact-task 查询路径已实现；不自动重复 uncertain business send | 补齐 provider effect 与子任务会计/终结组合；授权处置、不可证明结果保留 Unknown，重启后相同决定 |
| MCP/A2A 安全映射 | 现有两协议 guide、scope/resource 授权、租户上下文、URL/credential 约束和协议测试分散存在 | 独立 RFC，明确 ingress/egress、routing tenant 与本地 tenant、凭据 audience、descriptor/Agent Card 信任及恢复授权；RFC-0004 不能替代 |
| Issue 140 MCP stdio | 当前 `McpClient` 只实现 stateless Streamable HTTP；没有 stdio 支持声明 | 先固定 profile 与进程所有权 RFC，再复用 bounded parser/schema/tool 授权；实现 explicit executable/literal argv/env allowlist、握手与帧限制、request correlation 和完整后代回收 |
| 外部消费者 | crate 已发布 alpha.1，但后续源码功能不能用该包声明 | exact revision/version pin 的独立消费者，实际发现和调用；不自动安装 MCP 程序；平台只按执行证据声明 |

MCP stdio 验收必须包括启动失败、握手阻塞、版本不匹配、畸形/超大帧、stdout
非协议输出、stderr 洪泛、交错通知、重复/未知 response ID、caller drop、取消、
超时、server 退出、显式 shutdown 和子孙进程持有 pipe 的情况。
进程组杀死父进程并不自动证明已释放所有后代；必须根据实际支持的平台验证
完整生命周期机制。清理完成前不能发布该能力或关闭 Issue 140。

## 执行与关闭规则

R1 首先补齐 C2/C3 和 G3 的证据/语义，并保留 C5 注册/隐私回归，独立协议安全 RFC 与 stdio RFC 随后进入
实现评审。每个增量保留 DCO、必要回归、支持范围及中英文采用文档。
新增公开/持久化契约须先按 RFC 流程收口，不能通过修改本清单降低退出条件。

R2/R5/R6/R7 项有明确后续阶段，但仍是原 RFC 的整体验收门禁。R1 关闭表示
本阶段剩余语义与接入交付已完成，并不表示已有整套生产容量、稳定 API 或发布
认证；完整 RFC 接受和最终生产声明仍须等待所有相关证据。
