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
PR 161 的后续 CI fuzz 保留了 `MediaType` 大小写混合输入反例；reader 已支持
规范化而输入 schema 拒绝。[RFC-0023 草案](rfcs/0023-media-type-input-schema.md)
修正方向性输入 pattern，保留原读取行为、wire 和全部 314 个输出 pin；35 个
嵌套输入/default pin 显式变化，279 个无关输入 pin 保持一致。真实上一源码的
artifact 输入 schema、审查前后 pin 和 Tool 注册表零调用拒绝路径进入回归，
注册表升级须使用新 schema/Tool 版本并保留原 executable。最终源码验收仍须通过。
现有预览发布不构成稳定 API 承诺；不能以完成 C1 或目录检查接受整个 RFC。

## RFC-0002：确定性 Graph 与调度

编号按 [RFC-0002 的十一条验收项](rfcs/0002-deterministic-graph-and-scheduler.md#validation-and-rollout)排列。

| 编号 | 已有证据 | 仍需交付 | 阶段 |
| --- | --- | --- | --- |
| G1 | graph/checkpoint/barrier/node-attempt/node-result 的 schema 与 canonical fixture；新增完整 wire 对照与逐型 reader，覆盖节点四种 control、等待、终结和 model/tool 绑定 | 全部当前公开序列化类型已纳入 307-reader/308-schema 清单；保留新变体与完整性组合审查 | R1 |
| G2 | 既有 graph/barrier/recovery 顺序属性保留；新增两个独立模型，每个 256 个样例：随机节点插入/结果输入顺序，独立状态与 route 并集，后继 checkpoint 规范字节及状态/意图/记录三类摘要 preimage；一至十三代 Unicode 状态链核对父 head 和 journal anchor，并拒绝状态篡改 | 当前纯根 barrier/checkpoint 边界由 [#160](https://github.com/StateKnot/StateKnot/pull/160) 的完整本地及 14 项最终 CI 验收通过，合并 tree 与验收 tree 一致；相同已提交事实不等于不同 journal 历史，同 Run 嵌套及持久化故障另见 G3/G4 | R1 |
| G3 | 顺序、条件、并行、等待、取消、静态共享状态组合、有界循环的迭代/耗尽/等待恢复及多条真实数据库路径 | 已有有界循环的限定路径继续回归；同 Run 嵌套完整恢复由 [RFC-0022 草案](rfcs/0022-namespaced-graph-frames.md) 明确身份、独立 checkpoint、等待/父续接和复合事务 journal 绑定。[#161](https://github.com/StateKnot/StateKnot/pull/161) 持续开发中：六个实验性 Core 帧/调用/屏障 reader 加入封闭清单、双 schema pin、当前源码 fixture 及 fuzz oracle；九项确定性与两个各 256 样例的独立模型验证 scope、pin、七层真实激活及连续父链，调用声明进入编译图摘要，serial caller、固定 route、schema/owner、实际目标/深度/闭包字节及 executor 冲突有源级检查。scoped recovery 的四项检查验证作用域、同 fence 执行、接管及已提交结果复用；帧屏障另有五项检查和一个独立 256 样例归并/规范摘要模型，共享既有 schema/reducer/control 校验，旧根屏障继续拒绝非根结果。仅 `CompiledGraph` 与 `ChildRunAdmissionIntent` 的 schema profile 显式变化，旧根 wire/定义摘要保持一致；这不构成执行支持。调用/返回准备已有精确声明/目标/caller/base、隔离快照与 terminal 固定 route 校验，十一项构造器检查；迁移 27、显式根查询与 21 约束/8 列/7 索引的精确 scope catalog 已实现，PostgreSQL 16/17 已执行非空源码 schema 26→27 与 scope FK 检查。手工 scoped 行不证明准入/复合 journal 事务或历史 N-1/N-2。迁移 28 新增专用 Store 入栈，原子绑定实际 Root 准入、目标闭包、活动叶、祖先累计上限、共享 DIRECT/settled-child 用量与整个 event/start/checkpoint/head/stack；严格原始记录恢复先于新 schema/lease，SQL 拒绝不完整 scoped checkpoint、伪造 head、悬停父级执行及无 scoped owner 的旧等待。精确新 catalog 与 Schema 28/47 表角色白名单同步。帧内普通节点实际启动、成功/result 与失败原子提交已接通；完整重读、有界历史、接管、悬停作用域、框架 caller 拒绝、组件注入回滚和延迟约束后的租约复核有九项原生回归。提交 c4967d0 的 PostgreSQL 16/17 各通过 126 项 Store 原生与 140 项 Runtime 检查，包含强制健康业务备份恢复、独立 runtime LOGIN 的帧内启动/成功/重读；catalog 经两版 schema-only 逻辑恢复保持精确，stack CHECK 修复已验证。MediaType 修复提交 41bf0b8c 已通过全部 14 项 CI/依赖检查及本机/CI 精确 ASan 证据核对。迁移 29 接通真实 scoped 继续/终结屏障、完整 witness/消费与有界历史链；九项实际数据库检查覆盖竞争、所有组件故障、晚租约、预算下界、篡改、父续接及非空源码 28→29。Schema 29/48 表角色配置与精确 catalog 同步，提交 cc75c971 的全部 14 项 CI、1,862 项工作区检查、严格 Clippy/Rustdoc、331 项网站检查及 PostgreSQL 16/17 各 135 项 Store 原生和 140 项 Runtime 检查通过；真实 runtime LOGIN 验证作用域屏障提交/重试/重读，两版健康 Join 备份及精确 schema-only 恢复通过。本机/CI 均核对 2,297 份种子、三个目标各 10,000 次实际变异、相同源码/语料/锁；合并树与验收树一致。迁移 30 正在接通框架调用方物理接管，保留同一子帧并原子绑定新 start/claim/event，原预算、重试与 64 次硬上限不重置；最终源码全量验收仍待完成。迁移 31 新增完整返回事务，将已保存 terminal 证明与当前物理 caller 完成、精确父结果、stack 弹出和 journal 原子结算；后续父屏障推进原父状态，共享 DIRECT 下界不重置。事务内精简证明按 journal 顺序复用，默认栈七层级联、24 路竞争、全部组件回滚、晚租约、接管、损坏/绕过拒绝和非空源码 30→31 有九项原生回归；受限 LOGIN 完整返回/重试/重读路径已接通。精确 Schema 31/50 表角色配置同步，最终源码全量验收仍待收齐。迁移 32 已接通完整活动叶悬停：版本 2 的作用域屏障绑定原生命周期修订、全部审批/定时注册、Run Waiting 与租约释放；恢复验证完整审批、触发或放弃终态记录后才允许续接，损坏悬停仍可提交独立隔离审计。现有版本 1 屏障、根 wire、原 Core pin 与 50 表权限不变；24 路竞争、七类组件回滚、终态篡改拒绝实际节点启动、损坏隔离、晚租约及非空源码 31→32 回归已实现，最终精确源码双数据库/全量验收仍待完成。恢复快照新增单次认证完整活动叶与最多七层入口/检查点/当前 caller 紧凑绑定；默认栈七层逐层恢复、接管、隐藏 Root/虚增计数拒绝及等待取消后保留栈有真实数据库回归。DIRECT 仅为结构下界，读取不授予执行权限，完整源码资格验收仍待收齐。恢复快照提交 `b0bbee8` 已完成全部 14 项 CI/依赖检查、1,888 项本机工作区检查、严格 Clippy/Rustdoc 及 PostgreSQL 16/17 各 161 项 Store 原生和 140 项 Runtime 检查；PG16 首次协议错误与同源重跑分别保留。本机 ASan 三目标实际次数为 4,824/3,124/175，CI 各 10,000，均按原执行/时间上限与完整源码/语料/锁核对。当前 Schema 33 的整栈关闭增量保留实际旧 fence、原始取消/封存失败和冻结完整 DIRECT，原子保存所有关闭 caller 并回到 Root；根终态不能绕过打开的帧。真实竞争、逐组件/延迟故障、七层混合 fence、等待放弃、篡改拒绝、过期/预算耗尽清理和非空源码 32→33 升级已有原生回归；作用域 Tool/Model 准入/推进/历史正在补齐，未知效果不能当作已结算。精确 catalog 与 Schema 33/52 表角色配置同步。冻结 `f05d473` 的 whole-frame 限定资格，以及 `8285f25` 的 claimed scoped planning/精确 namespace 拒绝和私有 return 解码增量已分别完成完整门禁；[claimed 增量报告](qualification/experimental-claimed-frame-recovery-2026-10-10.zh-CN.md)记录 1,909 项工作区检查、双版 Store 各 182 项原生/Runtime 各 140 项、全部 14 项 CI/依赖检查、默认栈 Linux 七层级联及本机/CI ASan 原始源码证明。七个真实旧 return 的当前 reader 只读备份兼容不替代历史 N-1/N-2 程序或 ACL 恢复。scoped retry、多图完整应用 replay、Root Wait/Terminal 完整 disposition 持久化、实际 Driver 与 scoped child Join 仍开放；后续增量不得继承本轮源码资格。完整 driver 和进程/丢确认故障矩阵仍需实现，现有 flatten/root namespace 不替代嵌套执行 | R1 |
| G4 | 多组 commit-loss 与 post-commit OS-kill 专项 | pending result、journal、checkpoint、lifecycle、outbox 的每个写入边界完整矩阵及组合故障 | R1 矩阵；R6 完整验证 |
| G5 | ready-node 恢复、不可变成功结果复用、exact-key runtime/Agent 重放 | 将重试与 sibling 结果复用结合到所有新增组合路径，确保不重复已提交 model/tool 调用 | R1 |
| G6 | logical-result 冲突、compiled graph/schema pin、corruption quarantine | 新增 namespace 与组合路径保持同样的拒绝和隔离语义 | R1 |
| G7 | 限定次数的 stale-fence/race 测试 | 10,000 次强制租约竞态，旧 Worker 写入成功数为 0 | R6 |
| G8 | 持久化 wait 和索引化就绪发现 | 100,000 挂起任务，无逐任务常驻 task/polling，额外 scheduler 内存不超过规范阈值 | R6 |
| G9 | 加权调度实现、缩减 Host 测量工具 | 参考拓扑下公平性、饥饿界限、排队、恢复和过载报告 | R6 |
| G10 | 独立逻辑 restore 与物理 named-PITR drill | 同步 standby failover、完整参考数据集恢复和完整性检查 | R6 |
| G11 | populated migration、部分 checkpoint 恢复与拒绝测试 | 支持窗口内历史 graph/checkpoint fixture、升级中断和 rollback-limit 验证 | R1 契约；R6 升级 |

2026-10-16 的[Return 进程故障记录](qualification/experimental-frame-return-process-faults-2026-10-16.zh-CN.md)
补充 G4/G5/G6 的定向证据：PostgreSQL16/17 各通过 Store 中层组件损坏拒绝和
Runtime Return COMMIT 请求/响应丢失、真实 SIGKILL、受信强制 higher-fence 接管后的
冷 Driver 恢复，原叶结果没有重派发。六类本机门禁通过，原失败保留；这不是自然
租约到期、完整 Native/CI、一般进程树 owner 或完整故障矩阵，G3/G4/R1仍开放。

2026-10-15 的[原绝对 Graph Wait 采用记录](qualification/experimental-original-graph-waits-2026-10-15.zh-CN.md)
补充 G3/G5/G6/G10/G11 的限定证据：schema36 保留完整原 Pending、条件身份与
due，按实际提交事件时钟保存 future registration 或 elapsed evidence；全已到期
Timer 产生 Ready，不能据此直接派发。最终 922 文件源码的严格六项门禁、两版
PostgreSQL 四组原生 suite、强制逻辑恢复及独立 named-PITR 已实际通过，默认栈
七层恢复保留原租约。PITR 恢复断言通过后，PG16 留下一个本测试 WAL 卷；原 Drop
忽略清理失败，资源清理资格仍开放，不能把业务恢复的通过算作完整生命周期通过。
原失败、真实 schema35 writer 的 source32 重建和单个真实
alpha.1 升级各自保留来源与适用范围；这些证据不关闭完整历史兼容、scoped
Child Join、进程/COMMIT 故障矩阵、Linux 全部 CI 或 G3/R1。

2026-10-15 的[PITR 清理确认记录](qualification/experimental-pitr-cleanup-confirmation-2026-10-15.zh-CN.md)
仅补充 G10 的两个新 UUID 成功路径：独立922文件/test-only候选的定向 Runtime
Clippy、fresh189库存及 PostgreSQL16.14/17.11 各 exact1 实际通过，成功 marker 前
确认各两个命名容器和三个卷不存在。原预算、耐久断言和 best-effort Drop 保留；
旧 PG16 WAL 卷仍在，旧清理异常不重写，全部失败路径不据此获得资格。与 A2A
组成后的531输入 main 已在[后续原opt0六门禁记录](qualification/experimental-composed-request-facts-pitr-2026-10-15.zh-CN.md)
实际通过98个suite/2073 passed/6个原ignored及45个独立doc，全部2182个当时非ignored
文件前后相同；这仍不转授完整189/四套Native、CI或R1。

2026-10-15 的[测试 profile 限定执行记录](qualification/experimental-postgres-test-profile-2026-10-15.zh-CN.md)
补充组合531输入的本机test opt1原始执行：六门禁及双版原四套Native加单列facts共十suite
命令全部exit0，默认栈七层/serial/shared quantum和原marker完整保留；runner收尾因两新缓存
缺TAG而exit1，原失败保留，仅另有通过的只读空闲审计。head686原CI为11success/2cancelled，
双Store264通过但Runtime189无终态，原30m耗尽注解保留；同tree Linux PITR双版exact1
只按独立profile记录。本机feature union不转授原CI默认构建或30分钟整链；9行job env候选
不改原步骤、资源或预算，完整CI、G3/R1与Supported仍开放。

剩余语义决策：typed builder、reducer 注册/版本、非 root namespace grammar 与
shared-state declaration 属于 R1；参考负载下调度策略和阈值属于 R6。
blob checkpoint 若引入须另立 RFC，并先依赖 R2 的 Artifact 生命周期。

后续已采用opt1的head85f [CI38752172054](https://github.com/StateKnot/StateKnot/actions/runs/38752172054)
真实终态为11success/2原30m取消；两版Store264/Runtime189均终态通过，后续完整链未完成，
没有显式FAILED判定。该tree不含本MCP候选；新增MCP步骤及完整CI仍未执行。

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
| MCP/A2A 安全映射 | 现有两协议 guide、scope/resource 授权、租户上下文、URL/credential 约束和协议测试分散存在；[RFC-0025 草案](rfcs/0025-protocol-identity-and-authority.zh-CN.md)已独立映射 ingress/egress、远端与本地租户、audience、descriptor/Agent Card 信任和恢复授权。新增 A2A prepared method/URL/body 事实接口已有[限定采用记录](qualification/experimental-a2a-request-facts-2026-10-15.zh-CN.md)：26 项真实网络、双版各 6 项 durable 用例和完整严格静态门禁通过，拒绝及原回执恢复均核对实际发送次数 | 可信宿主数据释放/当前 consent 策略、MCP 绑定状态的真实部署/当前映射资格（组件证据见下行）、实际 verifier/持久 backend、部署与独立安全资格仍须验收；事实访问器不自动执行策略，Anonymous/旧 provider 不获得参数授权，RFC-0004 和本草案均不能替代完整资格 |
| MCP principal绑定状态数值修复候选 | [新限定记录](qualification/experimental-mcp-lossless-bound-state-2026-10-16.zh-CN.md)绑定928文件`69af3a0f…`/536输入`ded1a866…`，六门禁2098/0/6、48doc通过；fresh default-feature opt0双PG16.15/17.11各完整串行15/0/0/0和唯一marker通过。三ADv3、cursorv2、envelope1独立，旧v2无fallback；真实数值反例Tool可达、Prompt仍字符串schema。旧eedd/14项成功与反例并存，旧候选不能按exact argument binding采用 | 新Ubuntu/test-opt1 MCP/完整CI、宿主当前mapping/verifier/不复用generation及排队后检查的部署资格继续验收；fixture账本不代表全部表/backend，workspace条件分支/旧916不转授；Private不扩大应用Public内容保密，不关闭RFC-0025/R1，不交付stdio/backend |
| Issue 140 MCP stdio | 当前 `McpClient` 只实现 stateless Streamable HTTP；[官方规格与锁定源码研究](research/mcp-stdio-process-ownership-2026-10-12.md)已记录 SDK 无界接收、默认版本与进程回收限制；[RFC-0024 草案](rfcs/0024-owned-bounded-mcp-stdio.zh-CN.md)提出完整 session、目录准入、发送阶段、聚合限额和完整进程树生命周期，尚未实现或声明支持 | 先完成平台 owner、最终限额及可编译 Interface 的评审与原生验证，再复用 bounded parser/schema/tool 授权；实现 explicit executable/literal argv/env allowlist、启动版本核实与帧限制、request correlation 和完整后代回收 |
| 外部消费者 | crate 已发布 alpha.1，但后续源码功能不能用该包声明 | exact revision/version pin 的独立消费者，实际发现和调用；不自动安装 MCP 程序；平台只按执行证据声明 |

MCP stdio 验收必须包括启动失败、握手阻塞、版本不匹配、畸形/超大帧、stdout
非协议输出、stderr 洪泛、交错通知、重复/未知 response ID、caller drop、取消、
超时、server 退出、显式 shutdown 和子孙进程持有 pipe 的情况。
进程组杀死父进程并不自动证明已释放所有后代；必须根据实际支持的平台验证
完整生命周期机制。清理完成前不能发布该能力或关闭 Issue 140。

## 执行与关闭规则

2026-10-16 的 [scoped Wait 进程故障记录](qualification/experimental-scoped-wait-process-faults-2026-10-16.zh-CN.md)
补充双版 PostgreSQL 的真实提交截断、writer 终止、原 ACK 无写入恢复、双条件授权
及冷 Driver 后继/父应用各一次验证；仅三个测试路径变化。完整 Native/CI、elapsed
Timer、scoped child Join 等组合与 G4/R1 退出条件继续开放。

R1 首先补齐 C2/C3 和 G3 的证据/语义，并保留 C5 注册/隐私回归，独立协议安全 RFC 与 stdio RFC 随后进入
实现评审。每个增量保留 DCO、必要回归、支持范围及中英文采用文档。
新增公开/持久化契约须先按 RFC 流程收口，不能通过修改本清单降低退出条件。

R2/R5/R6/R7 项有明确后续阶段，但仍是原 RFC 的整体验收门禁。R1 关闭表示
本阶段剩余语义与接入交付已完成，并不表示已有整套生产容量、稳定 API 或发布
认证；完整 RFC 接受和最终生产声明仍须等待所有相关证据。

2026-10-18 限定增量：[scoped Join owner-stop Store 记录](qualification/experimental-scoped-join-owner-stop-2026-10-18.zh-CN.md)补充取消专用 closure-v2、schema37与当前54表部署及 v2 public schema getter；原普通 owner 保持 Executing，整个子账户的子级均已结算且 DIRECT 完整已定价才允许该精确 registered-unpublished profile。记录分别绑定实际源码/执行/迁移/冷读，不改写旧 schema33/35/36 或原 red 证据。该 Store 边界不关闭 Root ConfirmCancellation、scoped Driver Join、其他取消反例、完整故障/兼容/CI、G3/G4/D3 或 R1。

2026-10-19 限定增量：[G1 已发布 scoped ChildJoin 恢复记录](qualification/experimental-scoped-child-join-resume-2026-10-19.zh-CN.md)补充 G3/G4 的 published-unconsumed F2 owning preflight、原 publication 一次消费、真实 Return 与 Root Active handoff。`343b244` 的本机六门禁/20 次双 PG 定向和准确 head 的完整 run39228803615（21/21；两版 Store267/Runtime210）通过；该正向采用零 I/O child failure 前置和同进程重连，不证明 fresh child 应用或 OS-kill 恢复。G2 fresh/partial-spawn、完整故障/取消/兼容与 stdio 等仍独立开放，R1/R6/R7 不关闭。这里的 G1 是开发切片名，不改本表原 G1 schema 条目的编号或退出条件；旧 C9dd 的失败 CI、原37本地记录/portable bundle 与历史限定语境均保留。后续文档采用提交及 G2 源码不转授本次 CI 资格。


2026-10-19 G2 开发候选补充：[fresh scoped ChildJoin 采用与待验收记录](qualification/experimental-scoped-child-join-fresh-2026-10-19.zh-CN.md)给出实际 typed Exclusive fresh/partial ownership 接缝、稳定逻辑 slot/initial_state 与预算 headroom；reservation ceiling 不得作为spent，也不预加child usage。旧源码已分别实测partial、consumption、capacity与父真实Succeeded/childonce，旧admission双预算失败仍保留。同959/560基准已补Store实际构建/库存272与新caller/terminal并发双版通过，但v5、最终style组合的六门禁/完整Native与新CI仍待绑定；原Clippy101保留，不关闭G3/G4/D3或R1/R6/R7，不改既有G1 head343资格记录和RFC Draft状态。本段G2仍指开发切片，不重命名本表条目。


2026-10-20 G2 限定实证追加：[fresh scoped ChildJoin诊断](qualification/experimental-scoped-child-join-fresh-2026-10-19.zh-CN.md)保留全部旧source-specific事实。旧冻结final-v1=962/560的six-gates-v2全exit0（workspace2155 passed/6原ignored、doc48）；完整Native-v2 Store双版各272通过、Runtime17=216通过，Runtime16=215/1 partial原90秒readiness失败，稳定clock下无旧sleep干扰，完整组合仍未通过。同binary955d35d2…PG16单例47.081秒1pass/215filtered只作诊断，不能覆盖失败。seq22 Root节点succeeded/guardian续租仍不能区分首次cold drive末段与1316行repeat drive；不得称DirectEvidence唯一根因。拟议test-only同usage_at完整reader按身份复用，仍逐事件全keys唯一匹配、settlement及tenant/run/digest校验，新call重新完整parent/account/journal/head；不跨call缓存、不减live/quarantine/write guard、不扩90秒或预算/fence。新候选六门禁、完整双PG Native、发布与新CI待实际绑定，G1资格不转授，G3/G4/D3、RFC Draft与R1/R6/R7不关闭。


2026-10-21 限定增量：[published scoped Join 公开 Loop COMMIT 丢确认](qualification/experimental-published-join-public-loop-commit-loss-2026-10-21.zh-CN.md)补充 G3/G4/D3 的精确 publicLoop lifecycle 边界。旧已发布 `53b0d83`/source `fbc2dc9c…` 的完整 run39444455774 为22/22成功；新 minimal-v2 `1424fc42…`/compiled `54c23f83…` 的本机双PG16.15/17.11各1项定向通过仅证明四个cut：实际公开Loop经Driver handoff后写v3 closure，真实kill/reap，Before按持久原身份重建registry/完整DIRECT并等待原租约自然到期取得F3，After ID-only优先原ACK，完整tenant/租约及child once不变。`standalone_driver_mutation_interrupted=false`；新275/228只是实际构建库存，不是完整suite或CI验收。首轮宏语法构建101非功能RED且保留，新组合门禁/完整Native/CI、完整故障矩阵、R1/R6/R7及stdio仍开放；不改写前文历史条目或转授旧CI。


2026-10-21 D3 generic RunFailureClose限定修复：[实测记录](qualification/experimental-failure-close-real-tool-unknown-2026-10-21.zh-CN.md)给出真实Unknown/ReconcileFirst及childaccount组合退出边界。A在parentnodefailure真实保存、Run仍Active后逐请求精确Toolblocker无写；B在原failure-close及childRequestCancellation保存后精确UnsettledChildRuns无写，再经原invocation/schema/operation当前consent的真实provider对账，不业务重发。实际B的frozenDIRECT空币种与childUSD11产生正确total，却在原终态减法中保留USD0导致IncompleteChildAccounting；420a实RED+DB诊断保留。仅两私有Store路径正向验证frozenDIRECT+delegated并保留原DIRECT，Core明确zero语义及公开API/SQL未改。原测试2730保持，fixed972dcf/56388e的Root523ca双PG各1pass/228filtered、4profiles/116true，完整成本累计一次、child拓扑不继承、原ACK与wholeaccount重建后不写均有证据。该源Clippy仍101两helpers过长，style/CI/docs新组合须独立六门禁/完整Native/新CI；旧6ec与父53资格不转授。普通frame/noFailure/Rootbarrier同类问题、更广scopedUnknown/OSkill/外网effect、Driver中断、R1/R6/R7/RFC、stdio与发布仍开放，不修改原退出条件或历史限定结论。


2026-10-21 D4限定补证：[closed-frame冻结DIRECT记录](qualification/experimental-closed-frame-direct-costs-2026-10-21.zh-CN.md)补普通已关闭取消帧、无RunFailureClose且child引入币种的真实false rejection；三私有Store路径保留已认证原DIRECT，完整total正向核验、两来源全等与原锁序/错误优先级不减。当前16863bd8…/48eccc16…双版D4/D3及五个旧case共14次定向通过；同源Clippy仍101，完整六门禁/Native/发布CI未授。旧6baea426…四套full Native失败保留且不归当前源；wrong-total保护稿尚未运行，Root-barrier/更多Unknown与故障矩阵继续开放，不修改原R1/R6/R7、RFC与stdio退出条件。


2026-10-27 真实 Unknown 对账后父 finalizer 进程故障限定追加：[双语记录](qualification/experimental-real-tool-unknown-finalizer-commit-loss-2026-10-27.zh-CN.md)绑定既有隔离 ce3b2c6c…/564编译输入9b485402…：macOS aarch64 的 PostgreSQL16/17各exact1pass/230filtered，两个父finalizer COMMIT切点共4次真实writer SIGKILL/reap、72项前置检查true，随后身份限定新进程public tick与完整owning reads。Unknown已在kill前显式授权对账且child已结算；After空tick不是ACK，原完整event/account.observe_direct与全租户无写另有断言。生产源码不变；现行库存276/231及真实本机fixture不改写c9历史229或冒称Linux CI。b514基础上的新组合source/六门禁/完整Native/准确head CI待验；未解析Unknown跨进程、provider backend durability、Store Existing replay、跨OS kill、独立Driver mutation及R1/R6/R7/RFC/stdio/发布仍开放。


2026-10-27 后续验收：`6bf504e` 的真实 Unknown 对账后父 finalizer 测试组合已通过[完整 22 项 CI](https://github.com/StateKnot/StateKnot/actions/runs/40479323883)，包含当前 276/231 库存的双版本 Native 作业；此限定结果不关闭仍待补齐的故障矩阵。同日[私有 MCP Tool 结果解码](qualification/experimental-mcp-tool-outcome-boundary-2026-10-27.zh-CN.md)由真实 HTTP 采用，最终六门禁、双版各6项强制DB回归和冻结协议 required 门禁通过。新 MCP 交付仍须准确提交 CI；RFC-0024 的 owner、stdio 和外部消费者及 R1/R6/R7 继续开放。


2026-10-27 未解析 Unknown writer-loss 补充：[限定记录](qualification/experimental-unresolved-tool-unknown-writer-loss-2026-10-27.zh-CN.md)绑定真实 writer execute_tool 后 Unknown/reconcile-first、kill/reap、ID-only cold 进程自然租约到期后的更高 fence Driver handoff、显式 release 与之后明确 consent 恢复；完整账户、原 ACK、terminal delta 与分段无写窗口均有实际断言。992source f2089d5a…/565compiled7964eb40…六门禁及双PG各exact1pass/232filtered通过；真实库存276/233，CI同步保留历史229/231和原预算。新组合及准确head CI继续待验；Unknown事务COMMIT歧义、独立provider持久化、完整矩阵、R1/R6/R7与stdio仍开放。


2026-10-27 Tool Unknown 转换 COMMIT 限定补证：[记录](qualification/experimental-tool-unknown-transition-commit-loss-2026-10-27.zh-CN.md)分别证明 Before 的原 Executing1 与 After 的原 Unknown2，真实 writer/cold kill-reap、自然租约及空 provider 的原 attempt 恢复/新 attempt 拒绝，After 才显式 consent 对账与原 ACK。996source ccf157dc…/566compiled a2b025e7… 六门禁和双PG各exact1pass/234filtered通过，实际库存276/235；新CI动态118/117保留旧库存/原预算。该切片 child 仍 Active/unsettled，不声明 scoped-leaf、自动 Executing 授权处置或完整矩阵；补CI/文档新组合和准确tree CI待验，R1/R6/R7/RFC/stdio/发布不关闭。旧0d0d的22项CI成功与1018个case只属于其tree。
