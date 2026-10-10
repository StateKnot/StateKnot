<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 原始 claimed 观察下的 Root 与全生命周期帧应用重放

[English](scoped-graph-application-replay.md)

`ClaimedRunRecovery::validate_graph_replay` 使用实际部署的精确 schema 和纯 reducer，
独立重放原始 claimed 观察。`Some(leaf)` 要求当前活动叶与该完整 head 完全一致；
`None` 要求 Root 作用域，仍重放所有已返回帧。该 Store 边界为实验性实现；
RFC-0022 保持 Draft，实际嵌套 Driver 的集成和资格验收仍是独立必需项。

恢复开始会在返回会话前认证当前检查点的完整规范事实。若发现持久化规范数据
损坏，会按精确观测的日志头和当前有效栅栏隔离当前所有者；过期所有者或过期
会话不能隔离后继。合法旧历史缺失可恢复 Disposition 时仍返回运行错误。
本版不承诺旧实现的会话创建时序兼容。

## 精确依赖与完整覆盖

解析器接收完整、已认证的 `GraphReference`，包含规范定义摘要。Store 认证租户下
固定的完整图闭包，并要求实际 reducer 的完整 reference 等于编译图里的 pin。
不能选择最新版本或使用兜底 reducer；同一解析器可跨 claimed 会话复用。
schema 实现必须遵守固定摘要契约，reducer 必须确定、无副作用且有限。
捕获 panic 无法中断永不返回的同步 callback；Worker 续租和执行隔离仍需 Driver 实现。

重放覆盖 Root 初始及后续检查点、每个生命周期帧入口和屏障、非初始悬停父帧、
当前叶、已返回帧的完整父结果意图与物理 caller 完成、结果及调用归属和消费关系，
以及完整作用域等待注册、审批、触发或放弃证明。子返回先于父消费重放。
应用规划使用实际已提交 successor ID，比较完整 scoped 屏障、Disposition、
successor 和父返回意图；仅状态摘要一致不能通过。

Schema-34 的完整 Root bundle 认证原 lifecycle、source journal、投影及完整
Disposition。同一应用重放比较部署计划的完整 Disposition 与 successor，
capture、application、final 三轮均绑定完整 bundle 及已解决/放弃的 Wait 行。
Wait transcript 在事务内固定 UTC 与 ISO/YMD，连接设置不会改变相同持久事实
的摘要。新格式事件缺少 bundle 属于腐败；新格式不会补造合法旧记录未保存的
原 revision 或 Disposition。

旧 Root Continue 可认证完整屏障意图与精确 `Unchanged` 生命周期投影。
旧 generic Root writer 没有持久化原生命周期 revision 与完整 Disposition。
因此旧 Root Wait/Terminal，以及合法地使用其他 generic 生命周期投影的 Continue，
返回 `GraphReplayHistoricalDispositionUnavailable`。这是运行时兼容缺口：
不得捏造 revision、隔离合法旧记录或返回完整重放报告。只有真实留存的完整原事实
才能认证这些历史；升级 schema 不能恢复当时未保存的原 revision。

`GraphRecoveryReplayReport::root_report()` 返回现有的 Copy
`GraphReplayReport`，四个计数均来自同一次 Root 重放，不包含 scoped 帧。
该视图不会再次调用 callback 或单独执行 Root 重放。

## 事务、内存与工作量

capture 与 final 各自在一个 repeatable-read 事务中维护完整结构证明缓存，
缓存不能跨事务。应用阶段每条边只在新事务中认证直接组件，提交事务后才执行
解析器、schema 和 reducer。完整 owning 证明仍可能重新遍历 Root 检查点祖先，
尤其是 schema-34 Root bundle；总遍历复杂度尚未完成资格验收。
当前有界、事务内完整 Root 前缀证明是额外校验；原完整 initial 与 owning edge
路径全部保留。跨 callback 复制的紧凑 inventory 只作 transcript 相等对照，
不授予其他事务 owning authority。真实新 bundle 与旧 generic corpus、腐败反例
的等价对照不能代替原路径替换审批。
固定内存、独立 domain、长度前缀的摘要 transcript 绑定 capture、application、
final 三轮中所有组件的精确 head 与摘要。

| 资源 | 上限 |
| --- | --- |
| 打开的帧 | 七层 |
| 生命周期帧及返回 | 4096；只读紧凑前瞻，在完整解码第 4097 行前拒绝 |
| 图闭包 | 1024 个定义，规范定义字节最多 16 MiB |
| 紧凑生命周期 manifest | 实测字节最多 64 MiB |
| 单屏障紧凑结果 | `GraphReplayLimits` 默认 64 MiB，合法最大值 512 MiB |
| 状态与单个 JSON reader | 各自的 `JsonLimits::MAXIMUM` 上限 |
| 图工作量 | 各作用域最大步数及原准入 graph-step 预算，包含入口和返回结算 |

这些是不同边界，不能宣称整个进程内存为 64 MiB。跨 callback 只保留紧凑 head、
精确 pin 和结算绑定；单作用域 base/successor 与一组有界结果为临时值。
`structural_edges_verified()` 统计已埋点的结构证明及三轮直接边读取，
不包含每一次嵌套 owning 检查点遍历或 SQL 查询。
Root 8/16/32 条边实际访问为 24/48/96。七层级联有 13 条 scoped 屏障及七次返回，
访问 79 次，即 `5 × 13 + 2 × 7`：两轮完整证明各对每次结算多认证一次终结边。
这些计数不能证明 schema-34 历史的总工作量为线性。宣称这一性质前仍需完成
事务内完整 Root 前缀证明及等价、腐败反例验收。
Scoped 8/16/32 条边实际访问 40/80/160 次。两个已返回兄弟帧、两个 Root
continuation、七层打开作用域及六个非初始挂起父帧共访问 50 次，
即 `3 × 2 + 5 × 8 + 2 × 2`；十个应用屏障只执行十次 reducer。
当前叶合法时，旧 child 或 Root reducer 的变化仍会被发现。

## 最终观察与错误

整个读取仅进入一次原会话 quarantine wrapper。每条边和最终验证均复核原始
journal、完整 canonical lifecycle、调度及 wait 投影、Root 指针、精确叶，
以及数据库时钟下的原始 live fence。同一 fence 下的正常续租允许完成读取。
报告字段私有，只提供实测计数与精确 head，不授予执行权限。专用持久启动事务
仍是最终 fence；实际 Driver 必须在 handoff 前执行重放并拒绝任何失败观察。

| 条件 | 结果 |
| --- | --- |
| 跨 tenant/Run 的叶 | `InvalidClaimedRunRecoveryContext`，不隔离 |
| journal/lifecycle/调度/wait 投影/head/lease/fence 改变 | 原有 stale 或 authority 错误，不返回报告 |
| 精确依赖缺失、资源错误或 panic | `GraphReplayDependencyUnavailable`，不隔离 |
| 本地 manifest/结果字节上限 | `GraphReplayResourceLimit`，不隔离 |
| 旧 Root Disposition 无法认证 | `GraphReplayHistoricalDispositionUnavailable`，不隔离 |
| 数据库失败 | 原数据库错误 |
| 持久值被 schema 拒绝、完整证明或应用语义矛盾 | 使用原始精确 fence 尝试隔离 |

callback 若接管或令原 lease 过期，旧 context 无法隔离新 Worker。
读取不创建应用 event、节点 start、result 或 checkpoint，不修补历史。
管理连接故障注入仅用于测试，runtime 角色仍遵守不可变表权限。

真实 PostgreSQL 验收覆盖 update、七层打开/返回栈、弹回 Root 后消费、等待恢复、
精确依赖与可用性分类、callback 后 journal/fence 变化、单连接池与独立 Run 锁、
capture 后组件改变和实际线性访问次数。该边界不完成实际嵌套 Driver、child/Join、
provider 外部效果结算、进程/丢 ACK 矩阵或 R1 资格。
等待验收包含恢复后实际提交的新 terminal 屏障：原 wait 只重放一次，不生成额外
恢复检查点。原投影变化返回 stale，同一 fence 下正常续租仍能通过。
