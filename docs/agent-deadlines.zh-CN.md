<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent 截止时间的耐久取消

[English](agent-deadlines.md)

PostgreSQL/runtime 配置现在提供基于数据库时间的截止时间协调，覆盖**已正式准入**的
根 Run 和子 Run。到期只会请求协作取消，不代表外部操作已经停止或成本已经确定。
没有 Agent 准入记录的底层启动 Run 不在此配置内。[失败关闭](failure-close.zh-CN.md)
现在保留先前失败决定；完整耐久子 Run 配置验收仍是独立门槛，不能宣称整个框架已达到生产发布条件。

## 宿主接入

先向离线 `JsonSchemaRegistryBuilder` 注册
`register_standard_agent_deadline_event_schema`，冻结后，用连接池、Schema 注册表和
已验证的 `DurableGraphLifecycleOptions` 构造 `DurableAgentDeadlineReconciler`。
该类型的 Rustdoc 有经过编译验证的配置和有限 tick 示例。

宿主调度 `tick(authorized_tenant, cursor, shutdown)`，同时运行独立租约的执行/清理
Worker、`DurableChildReconciler`，以及按需启用的 `DurableChildJoinPublisher`。
即使租户没有可执行 Run，也必须运行截止时间维护：Waiting、延迟重试、活租约和未发布
的子 Join 都不能豁免已准入的截止时间。构造器不会偷偷启动后台任务。

每轮最多处理 16 个索引候选；逐项返回错误，并继续后续候选。沿用生命周期有限重试
配置，默认 3 次、25 ms 起始指数退避、单次退避上限 1 秒；配置硬上限为 10 次。
一项重试复用相同事件/失败 ID。进程丢失提交响应后，即使用新 ID 重试，也会读取已经
赢得竞态的取消原因，不重复提交或改写原因。

跨 tick 保留 `AgentDeadlineSweepCursor`，包括某些条目失败时；短页结束后自动开始
下一遍扫描。进程重启从 `None` 开始，不能持久化一个永不回退、跳过旧失败记录的
截止时间水位。运行时和底层游标都拒绝跨租户复用。协作关闭保留最后一个完成候选；
关闭可能与数据库提交竞态，下一遍仍会按耐久生命周期收敛。发现查询失败不推进游标。

认证、租户维护授权、公平性、运行频率、关闭、错误处理和告警均由宿主负责。
必须检查每项结果，并监测整遍扫描耗时、最老到期候选、取消到结算的延迟、隔离项、
重试耗尽以及用量/外部效果证据不可用。此接口不承诺固定到期响应时间或容量/SLA。

## 原子语义

迁移 23 将不可变准入预算的有限截止时间投影到 `runs.agent_deadline_at`。
`(tenant_id, agent_deadline_at, run_id)` 部分索引只包含 pending/active/waiting，
进入取消或终态后自动退出索引。隔离的到期 Run 仍返回明确错误，不会悄悄从监督中消失。
发现候选不占用执行租约。

`request_agent_deadline_cancellation` 锁定 Run，验证准入规范编码及图、初始事件、
Checkpoint 锚点，验证等待证据和截止时间投影，然后读取**获得锁之后的数据库时间**。
恰好等于截止时间即到期；调用者不能提供时钟或扩大期限。新隔离写入拒绝执行；已经
提交的终态或其他取消原因优先保留，历史取消读取不重复计费。

一个事务写入 `agent-deadline-cancellation-requested` 审计事件、
`CancellationRequested`、所有真实等待的放弃记录和直接子任务的取消队列凭据。
任一环节失败则整笔回滚。独立离线 Schema 只记录操作、准入摘要、截止时间、失败 ID，
没有输出、凭证或私有诊断。公开原因固定为 `agent.deadline.expired`，类别 `Cancelled`，
重试建议 `Never`，明确区别于用户主动取消。

Run 行锁与生命周期/派生写入串行化；此事务不获取树/祖先锁，也不调用外部 Provider。
子取消交付在后续事务按既有“树 → 父 → 子”顺序执行。取消先提交则拒绝新派生，已经
提交的子任务则进入队列。交付不会伪造取消确认；父任务必须等所有子任务完成真实、
成本已知的终态结算，才能重新获取清理租约并关闭。取消历史可以保留未消费的成功 Join。

执行 Worker 沿用已有取消观察与耐久清理。用量未知、外部效果不确定时保留未解决工作，
不能把缺失证据写成零、把物理尝试伪造成成功或消费成功 Join。父级只累计一次已经结算
的子用量。此维护器不是强制终止开关：预算/派发检查、Provider 对账仍然必需，已经
开始的外部效果可能在截止时间之后完成。

## 升级与验收

已发布迁移 1–22、现有序列化契约和审计 Schema 不变。迁移 23 在事务内回填已有准入，
不改写原日志、Checkpoint 或结果。准入 INSERT 触发器也覆盖使用旧准入 SQL 的已连接
兼容写入方；Run 守卫拒绝删除/扩大既有截止时间。启动时除了迁移校验和，还校验确切的
列、约束、索引、启用的触发器定义及函数体。这保护可信 SQL 角色的兼容性与完整性，
不是对数据库管理员的安全隔离。

请安排维护窗口：新增迁移需要表锁、回填和普通索引构建。先在有代表性的数据副本上
测量耗时，准备可恢复备份，迁移期间停止调度，再启动兼容 Worker 和租户维护任务。
不能编辑已发布迁移或删除证据作为回滚方式；测试中的降级脚本仅限隔离升级夹具。

分别在 PostgreSQL 16、17 执行强制真实数据库测试：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='<隔离 PostgreSQL URL>' \
cargo test -p stateknot-runtime --test postgres --locked deadline_ -- --test-threads=1
```

覆盖未到期/到期、并发与丢失响应恢复、锁等待后的时间判定、清理证据不可用、真实定时器
放弃、挂起 Join 取消/队列回滚/进程重启/子用量累计、17 项分页越过错误、租户游标约束、
关闭与隔离、首个取消原因/终态保留、已有 v22 数据升级、索引可用性和篡改/禁用守卫拒绝。
这些使用确定性执行器和可信测试用量，不是在线模型或生产容量测试。
