<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 原始绝对 Graph Wait 恢复

这是 [RFC-0022 草案](rfcs/0022-namespaced-graph-frames.md#recovering-original-absolute-wait-conditions)下未发布的实验性 schema36 契约，`0.1.0-alpha.1` 没有这些接口。完整 CI、故障/兼容及生产接受仍开放。[English](elapsed-graph-waits.md)

## 提交原始条件

成功 Pending 结果可能保留到绝对 Timer 到期之后。恢复保留完整原结果、`NodeWait`、ID、kind 与 due，不重新执行成功节点、不改 due、不回填更早注册或伪造 firing。

分类使用完整 Barrier 事件真实数据库 `recorded_at`：

| 原条件 | 已提交证据 |
| --- | --- |
| Timer `due_at > recorded_at` | 既有 future-only 注册，保留原规格 |
| Timer `due_at <= recorded_at` | 不可变 elapsed 条件证据，没有 Timer 注册或 firing |
| 有效 Interrupt | 原授权注册与 resolver 策略 |
| 已过期 Interrupt | 显式拒绝，不自动批准或部分提交 |

每个条件绑定原语义序号、完整 Pending owner、owner 原 Wait 数组内位置、规格/注册 intent 摘要、source journal 与实际 successor/event。大 Interrupt payload 留在完整认证的原 Pending 中，紧凑库存不能替代 owner。原 64 条件、scoped 4 MiB 与 whole-transcript 上限保持不变。

## 处理 Waiting 与 Ready

完整 Root/scoped receipt 的 `wait_effect() -> Option<GraphWaitEffect>` 对非 Wait 返回 `None`，对原 Wait 返回认证后的 `Waiting` 或 `Ready`。

只要还有真实注册条件阻塞，事务便仅用这些注册进入 Waiting 并释放原租约；部分 resolution 与真实 firing 保留现有授权和最后条件唤醒契约。

全部原 Timer 已到期时，同一事务保存原 Wait successor、每个原结果的一次消费及全部 elapsed 证据，Run 保持 Active、原 revision 与完整有效原 lease。Ready 是持久观察，不是 launch 权限。

Scoped Driver 更新精确持久观察，在原共享 quantum 内，先取得 fresh claimed plan，再提交下一独立 durable Start。Root lifecycle 与 `DurableAgentLoop` 返回 `ReadyReleased { receipt, release }`：完整原 Root receipt 以及原 fence 的 normal release 结果。`release: None` 表示原 fence 已被替换，不能释放新 lease。后续业务由 scheduler discovery 与新 claim 继续。调用者须处理这一新增 non-exhaustive outcome，不能把它当成 Waiting 或终态成功。

丢 ACK 恢复首先认证完整原事务及库存，再返回原分类。后续时间、resolution、更高 fence 或 Return 都不能重新分类、二次消费、重置生命周期或授权新 launch。Fresh 写在 component guards 后重读实际完整行，在同一锁内最终复核原 lease、数据库时钟、deadline、journal、head 与 accounting。

## 迁移与部署

迁移36新增不可变 `graph_wait_conditions`，物理 `(tenant_id, wait_id)` 键跨 Run 及 Timer/Interrupt 种类仲裁旧注册和新事实。`legacy_registration` 只回填真实旧注册元数据/锚，不捏造 graph owner；`graph_registered`/`graph_elapsed` 绑定完整原 graph 事实。冲突必须精确匹配实际 winner，冲突后看不到 winner 也是失败。

已有 Agent Root V1、trusted Root V2、scoped V1/V2 字节及 owning reader 保持不变。新 V3 Wait 绑定完整原来源、plan、库存与 effect；缺 V3 库存属于腐败，不给合法旧历史伪造库存。Root 来源仍认证完整 Agent admission 或原 FIRST/Start/INITIAL 历史。

1. 排空旧 Worker 并关闭旧池，保留经验证备份和精确旧 binary。
2. 当前实验性 binary 由 migration owner 使用正常 SQLx 完整升至37，保留原 checksum/字节。非空源码 schema 重建测试与实际发布版本测试是不同证据。
3. 安装匹配当前源码的 trusted-role profile，验证精确 schema37、全部54业务表及 ACL。迁移37改变关闭 guard，保留 schema36 表/ACL 库存；不能复用旧版本配置。
4. 启动匹配 reader/writer；catalog 不符便拒绝运行。旧 V1/V2-only reader 不能服务新 V3 事实，这不是 mixed-version rolling upgrade 或自动回滚。

独立的 [schema37 Store Join owner-stop 记录](qualification/experimental-scoped-join-owner-stop-2026-10-18.zh-CN.md)
说明仅用于取消的 closure-v2 增量。迁移36仍是原 WaitV3 引入版本；当前完整部署至37
不会把历史资格转授给后续源码。

Restore 后仍须通过同样完整 catalog 与 owning reader。静态门禁、条件资源测试入口、源码重建 schema32、单一真实发布 alpha.1 升级各有独立范围，均不能单独授予完整兼容窗口或 R1 资格。证据须记录精确 source、binary、原资源、实际 marker 与全部原失败。
