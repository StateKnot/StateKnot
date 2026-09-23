<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 子 Agent 取消投递 COMMIT 丢失验收

[English](child-cancellation-commit-loss-qualification.md)

`child-cancellation-delivery-commit-loss-v1` 验收正式
`PostgresStore::deliver_child_cancellation` 事务的客户端歧义 COMMIT 边界。
测试中的子 Run 已通过真实 Timer 持久化进入 Waiting。一个事务必须同时请求取消、追加精确
Control-plane 事件、废止完整 Wait 集合、清除 Wait 投影、恢复清理调度资格、写入不可变投递
Receipt，并消费父级取消队列项。

本配置不向生产代码加入故障钩子，也不使用 SQL Trigger、事务 Mock 或数据库服务器崩溃。
每一格都有新的租户、操作系统进程、单一 PostgreSQL 连接和事务；恢复只允许由新进程通过
PostgreSQL 重建，控制器只通过独立连接读取证据。

## 强制矩阵

| 切断点 | 必须满足的结果 |
|---|---|
| `commit_not_forwarded` | 包括 Receipt 插入在内的全部投递语句均已到达服务器，但代理扣留前端 COMMIT。独立读连接此时必须仍看到原始 Waiting Run、未解决 Wait、空 Receipt 与待处理队列项。`SIGKILL` 并断开 Backend 后，完整快照必须保持不变；新进程随后只提交一次完整投递。 |
| `commit_response_withheld` | COMMIT 已到达 PostgreSQL，代理读取 `CommandComplete(COMMIT)` 与空闲 `ReadyForQuery` 后不向客户端转发。独立读连接必须看到完整取消与 Wait 废止集合。新进程使用新的事件和 Failure 候选值重试，但 `Idempotent` 恢复必须返回原始 Receipt 身份，且不得改变任何持久化字段。 |

对比快照覆盖父子 Run 的生命周期、日志头、租约、调度、Wait 集合和检查点投影；归属与结算；
累计预算；取消来源与 Receipt；待取消/待结算队列；完整子日志以及 Timer abandonment 事实。
部分取消、孤立 Wait、重复事件或 Receipt 身份被替换都会使验收失败。

恢复后，新 Worker 必须能认领取消请求中的子 Run；其终态确认必须只产生一次子结算，随后父
Run 必须能进入已确认取消。这验证歧义投递不仅静态一致，而且仍能完成正式清理闭环。

## 测试插桩边界

测试专用代理只接受一个明文 PostgreSQL 3.0 会话，并拒绝字面量回环地址或 `localhost`
之外的目标。封闭事务目标枚举只在命中精确
`INSERT INTO stateknot.child_run_cancellation_receipts ` Parse 前缀后启用，并切断下一条精确
简单查询 `COMMIT`。代理限制 Frame 大小、保留部分读取状态、转发启动/认证/Bind 流量，
且绝不记录协议 Frame。

协议异常、目标未命中、Worker 提前退出、证据不完整或非回环数据库都会失败关闭。控制器
只强制终止自己创建的测试进程，关闭对应代理会话，并等待捕获的 PostgreSQL Backend PID
消失。Unix 必须观察到 `SIGKILL`。本配置固定 SQLx 0.8.6 的事务交换；驱动交换或目标 SQL
变化时必须重新验收。生产 TLS 与数据库流量不会通过该代理。

## 复现与证据保留

使用一次性、仅回环可达的 PostgreSQL 16 或 17 数据库：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::child_cancellation_commit_loss::child_cancellation_delivery_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

两个强制 PostgreSQL CI Job 都要求恰好两条
`STATEKNOT_CHILD_CANCELLATION_COMMIT_LOSS_EVIDENCE` 记录并成功退出。CI 将
`child-cancellation-commit-loss-postgres-<version>-<run-id>` 保留 30 天，其中包含验收日志、
Source/Tree 身份、Lockfile 摘要、工具链、PostgreSQL 镜像、内核和宿主清单；数据库 URL、
凭证、Agent 输入与输出不会进入证据字段。

## 仍未覆盖的门禁

这是客户端进程故障配置，不是 PostgreSQL Server/WAL 故障测试。独立的
[子 Run 结算配置](child-settlement-commit-loss-qualification.zh-CN.md)现已覆盖结算。
独立的 [父 Run 终态收口配置](parent-finalization-commit-loss-qualification.zh-CN.md)
现已覆盖收口。Join 结果消费仍缺少同等级的 COMMIT 进行中切断。
不确定 Provider 副作用/计价、Failover、
PITR/恢复、不可信 Worker SQL 隔离、同 Run 嵌套命名空间、历史保留容量、延迟和 Soak
也仍未验收。因此 RFC-0004 继续保持 Draft，完整可恢复子 Run 能力不能宣称生产就绪。
