<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 子 Run 结算 COMMIT 丢失验收

[English](child-settlement-commit-loss-qualification.md)

`child-settlement-commit-loss-v1` 验收正式
`PostgresStore::settle_child_run` 事务的客户端歧义 COMMIT 边界。子 Run 已持久化为
费用完整可知的 Failed 终态，之后还有一条不改变生命周期的审计事件。结算事务必须绑定
原始终态事件、只追加一次父级审计事件、把未结预算预留替换为不可变的子树实际用量、
保存唯一结算事实，并原子地移除待结算通知。随后父 Run 必须按该用量恰好记账一次并收口。

本配置不向生产代码加入故障钩子，也不使用 SQL Trigger 或事务 Mock。每格都有新的租户、
子 Run、Worker 进程、单一 PostgreSQL 会话和事务；新进程只通过 PostgreSQL 重建，
控制器通过独立连接读取证据。

## 强制矩阵

| 切断点 | 必须满足的结果 |
|---|---|
| `commit_not_forwarded` | 包括结算记录插入在内的全部语句已到达 PostgreSQL，代理扣留前端 COMMIT。独立读连接仍必须看到原待结算通知、预算预留、父日志，且不存在结算记录。`SIGKILL` 并断开 Backend 后，完整快照保持不变；新进程只提交一次结算。 |
| `commit_response_withheld` | COMMIT 已到达 PostgreSQL，但代理读取回执后不转发。独立读连接必须看到完整结算、预算账户转换、父级审计和通知移除。新进程以新的候选事件重试，幂等路径必须返回原始事件且不得改变任何持久化字段。 |

对比快照覆盖父子 Run 的生命周期、日志头、租约、调度、Wait 和 Checkpoint；不可变归属
及结算；完整预算账户；通知列表；父子完整日志；以及原始归属行的待处理/已结算标志和
结算行数。部分结算、重复计费、替换事件身份或丢失通知都会使验收失败。子 Run 在终态
后另有审计事件，因此结算必须保持原始 Failed 事件为终态锚点，不能误用最新日志头。

恢复后，以零子用量关闭父 Run 必须被拒绝；父 Run 随后合入精确子用量并进入 Failed，
且子结算保持不变。这同时验证歧义提交后的静态原子性和后续运行闭环。

## 测试插桩边界

测试专用代理只接受一个明文 PostgreSQL 3.0 会话，并拒绝非回环目标。封闭事务目标枚举
只在命中精确的 `INSERT INTO stateknot.child_run_settlements ` Parse 前缀后启用，
切断下一条精确简单查询 `COMMIT`。代理限制 Frame 大小、保留部分读取状态、
转发启动/认证/Bind 流量，且绝不记录协议 Frame。

协议异常、目标未命中、Worker 提前退出、证据不完整或非回环数据库都会失败关闭。控制器
只强制终止自己创建的 Worker，关闭代理会话，并等待捕获的 PostgreSQL Backend PID
消失。Unix 必须观察到 `SIGKILL`。本配置固定 SQLx 0.8.6 的事务交换；
驱动交换或目标 SQL 变化时必须重新验收。生产 TLS 与数据库流量不会经过该代理。

## 复现与证据保留

使用一次性、仅回环可达的 PostgreSQL 16 或 17 数据库：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::child_settlement_commit_loss::child_settlement_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

两个强制 PostgreSQL CI Job 都要求恰好两条
`STATEKNOT_CHILD_SETTLEMENT_COMMIT_LOSS_EVIDENCE` 记录并成功退出。CI 将
`child-settlement-commit-loss-postgres-<version>-<run-id>` 保留 30 天，其中包含
验收日志、Source/Tree 身份、Lockfile 摘要、工具链、PostgreSQL 镜像、内核和宿主清单；
数据库 URL、凭证、Agent 输入与输出不会进入证据字段。

## 仍未覆盖的门禁

这是客户端进程故障配置，不是 PostgreSQL Server/WAL 故障测试。父 Run 终态收口与
Join 结果消费仍缺少同等级的 COMMIT 进行中切断。不确定 Provider 副作用/计价、
Failover、PITR/恢复、不可信 Worker SQL 隔离、同 Run 嵌套命名空间、历史保留容量、
延迟和 Soak 也仍未验收。因此 RFC-0004 继续保持 Draft，完整可恢复子 Run 能力
不能宣称生产就绪。
