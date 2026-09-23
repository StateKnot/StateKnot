<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 父 Run 终态收口 COMMIT 丢失验收

[English](parent-finalization-commit-loss-qualification.md)

`parent-finalization-commit-loss-v1` 验收正式
`PostgresStore::complete_run_failure_close` 事务在客户端无法确定 COMMIT 结果时的
恢复能力。父 Run 已持久记录原始失败决策与完整、已计价的直接用量；一个真实归属的子 Run
已收到取消、完成物理清理，并以独立计价的子树用量结算。最终事务必须原子地追加唯一收口事件、
以直接用量加子树用量将父 Run 置为 Failed，并把原始失败收口记录标为完成。

测试没有增加生产故障钩子、SQL Trigger 或事务 Mock。每格使用独立租户、子 Run、
单连接代理和 Worker 进程。被中断的 Worker 被强制结束后，新进程从 PostgreSQL
重建状态。写入者持有父 Run 行锁期间，独立读者使用不加锁的 MVCC 查询。

## 必测矩阵

| 切断位置 | 必须观察到的结果 |
|---|---|
| `commit_not_forwarded` | 收口事件插入和父 Run 投影更新已送达 PostgreSQL，代理扣住客户端 COMMIT。独立读者仍看见 Active 父 Run、待处理收口、未变的 Journal 和已结算子 Run。`SIGKILL` 与数据库后端断开后，完整快照不变；新进程只提交一次收口。 |
| `commit_response_withheld` | COMMIT 已送达 PostgreSQL，但代理吞掉响应。独立读者看见 Failed 父 Run、精确终态用量、原始失败、唯一收口事件及已完成的收口记录。新进程提交新候选事件时取得 `Existing`，原始终态事件保持不变。 |

对比快照包含父子 Run 的完整投影、原始收口行、不可变归属/结算和取消 Receipt、完整预算
账户、待办队列、父子完整 Journal，以及原始收口行数和完成行数。子 Run 结算、原始失败和
登记时的 Journal Head 不得改变。再次重放不能修改任何持久字段。恢复后常规失败收口扫描
必须没有待处理候选。

## 注入边界与证据

仅测试使用的代理只接受一个明文 PostgreSQL 3.0 连接，拒绝非 Loopback 目标。封闭的
事务目标枚举仅在父 Run Journal Head 投影 `UPDATE` 的精确 Parse 前缀后武装，并切断
下一条精确的简单查询 `COMMIT`。代理限制 Frame 大小、保留分段读取状态、不记录协议
Frame，并固定 SQLx 0.8.6 的事务封包行为。异常封包、目标缺失、Worker 提前退出或
证据不足都直接失败。控制器只结束自己创建的进程，并等待所记录的 PostgreSQL 后端 PID
消失；Unix 环境要求真实 `SIGKILL`。正式环境的 TLS 与数据库流量不会经过此代理。

使用可丢弃的本机 PostgreSQL 16 或 17 数据库复现：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::failure_close::completion_commit_loss::failure_close_completion_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

两个强制 PostgreSQL CI Job 都要求恰好两条
`STATEKNOT_PARENT_FINALIZATION_COMMIT_LOSS_EVIDENCE` 记录并成功退出。CI 将
`parent-finalization-commit-loss-postgres-<version>-<run-id>` 保留 30 天，包含
验收日志、Source/Tree 身份、Lockfile 摘要、工具链、PostgreSQL 镜像、内核和宿主清单。
数据库 URL、凭证及 Agent 输入/输出不会进入证据字段。

## 仍未覆盖的门禁

这是客户端进程故障验收，不是 PostgreSQL Server/WAL 故障测试。Join 结果消费仍缺
同等级的 COMMIT 进行中切断。不确定 Provider 副作用/计价、Failover、PITR/恢复、
不可信 Worker SQL 隔离、同 Run 嵌套命名空间、历史保留容量、延迟和 Soak 仍未验收。
RFC-0004 继续保持 Draft，不能宣称完整可恢复子 Run 能力已生产就绪。
