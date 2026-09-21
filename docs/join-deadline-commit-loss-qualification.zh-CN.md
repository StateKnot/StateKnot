<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Join 与 Deadline 的 COMMIT 丢失验收

[English](join-deadline-commit-loss-qualification.md)

`join-deadline-commit-loss-v1` 补齐三个正式事务的客户端歧义提交验证：子 Join 注册、
子 Join 发布，以及父 Run 挂起在该 Join 时的 Deadline 取消。测试不向生产代码加入
故障钩子，不使用 SQL Trigger 或事务结果 Mock，也不把客户端断线冒充数据库服务器崩溃。

## 强制矩阵

每一格都使用新的租户、操作系统进程、PostgreSQL 连接和事务。控制器通过独立连接读取
持久化状态；Worker 的任何内存状态都不能充当恢复证据。

| 操作 | `commit_not_forwarded` | `commit_response_withheld` |
|---|---|---|
| Join 注册 | 所有语句执行完毕，但代理扣留完整的前端 COMMIT。杀死客户端后，Join 事件、封存成员和租约释放必须一起回滚；新进程随后只提交一次。 | COMMIT 已到达 PostgreSQL，但客户端收不到 `CommandComplete(COMMIT)` 与空闲 `ReadyForQuery`。新进程必须以 `Idempotent` 取回原注册；成员、事件身份和已释放租约均不能变化。 |
| Join 发布 | 子终态结算已经持久化。未转发的 COMMIT 不能暴露 Binding、发布事件或调度唤醒；冷恢复必须把三者原子提交，并使父 Run 可认领。 | Binding、发布事件和调度唤醒在阻塞客户端被杀前已可独立读取。带新候选事件的恢复必须返回原始 `Idempotent` 发布，且日志不能前移。 |
| Deadline 取消 | 已到期父 Run 正挂起等待封存 Join。未转发的 COMMIT 必须一起回滚审计事件、首个取消原因、生命周期迁移和直接子取消队列；新进程只提交一次完整集合。 | 客户端仍在等待时，独立连接已能读取完整取消集合。恢复使用新的事件/失败 ID，必须返回 `AlreadyRequested`、保留 `agent.deadline.expired`，且不能新增第二个队列项或日志事件。 |

最终快照包含父子生命周期、日志、租约与调度状态、子归属/计费、物理尝试、Join 证据、
取消回执，以及待处理 Join/取消/结算队列。测试还强制验证后续进展：恢复的注册能够结算并
发布，恢复的发布使父 Run 可认领，恢复的 Deadline 取消能够由正式子协调器投递。

## 测试插桩边界

测试专用代理只接受一个明文 PostgreSQL 3.0 会话，目标必须是 `localhost` 或字面量回环
地址。事务目标来自四项封闭枚举；本配置使用 Join 注册、Join 发布和 Deadline 投影校验
的精确 Parse 前缀。代理命中目标语句后，只切断下一条精确的简单查询 `COMMIT`。

启动、认证和 Bind Frame 会被转发但绝不记录。Frame 大小有上限，部分读取不会丢失状态；
协议交换异常、Frame 截断、目标未命中、Worker 提前退出或矩阵不完整都会失败关闭。控制器
只强制终止自己创建的 Worker，关闭该代理会话，并等待对应 PostgreSQL Backend PID 消失。
Unix 必须观察到 `SIGKILL`；正常退出不能作为证据。

本配置固定 SQLx 0.8.6 的事务交换；驱动交换或目标 SQL 变化时必须重新验收。生产 TLS 和
数据库流量不会经过此代理。

## 复现与证据保留

使用一次性、仅回环可达的 PostgreSQL 16 或 17 数据库：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::transaction_commit_loss::join_and_deadline_commit_loss_are_recoverable \
  --nocapture --test-threads=1
```

两个强制 PostgreSQL CI Job 都要求恰好六条
`STATEKNOT_JOIN_DEADLINE_COMMIT_LOSS_EVIDENCE` 记录和成功退出。CI 将
`join-deadline-commit-loss-postgres-<version>-<run-id>` 保留 30 天，其中包含验收日志、
Source/Tree 身份、Lockfile 摘要、Rust 工具链、PostgreSQL 镜像、内核和宿主清单；不会
输出数据库 URL、凭证、Agent 输入或子输出。

## 仍未覆盖的门禁

这是客户端进程故障配置，不是 PostgreSQL Server/WAL 故障测试。它没有在子准入、取消
投递、结算、终态收口或 Join 结果消费的 COMMIT 进行中切断，也不验证不确定 Provider
副作用/计价、Failover、PITR/恢复、不可信 Worker SQL 隔离、同 Run 嵌套命名空间、
历史保留容量、延迟或 Soak。因此 RFC-0004 仍为 Draft，完整可恢复子 Run 能力仍不能宣称
生产就绪。
