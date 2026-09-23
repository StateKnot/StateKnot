<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 子 Run Join 结果消费 COMMIT 丢失验收

[English](child-join-consumption-commit-loss-qualification.md)

`child-join-consumption-commit-loss-v1` 验收正式
`PostgresStore::succeed_node_attempt` 事务消费已发布 Join 结果时，客户端无法确定
COMMIT 结果的恢复能力。真实子 Run 已准入、关闭并结算；Join 已注册并发布；父 Run
已有带 Fence 的物理节点尝试。该事务必须原子地追加唯一 Worker 事件、持久化精确的
Pending Node Result 和节点尝试完成记录、推进 Run Journal，并将 Join 标记为已消费。
不使用生产故障钩子、事务 Mock 或测试专用 Trigger。

## 必测矩阵

| 切断位置 | 必须观察到的结果 |
|---|---|
| `commit_not_forwarded` | Pending Result INSERT 已送达 PostgreSQL，但代理扣住客户端 COMMIT。独立读者看到未变化的 Run、Join、节点尝试、结果和完整 Journal。`SIGKILL` 加数据库后端断开后，整笔事务回滚；新进程只提交一次。 |
| `commit_response_withheld` | COMMIT 已送达 PostgreSQL，但代理吞掉响应。独立读者看到恰好一个新事件、已完成尝试、匹配的 Pending Result 和已消费 Join。新进程提交新候选事件，幂等取回原始已提交事件，持久证据不变。 |

两格均使用重建的 Registry 和 `DurableGraphDriver`，通过持久化结果推进真实的
非初始 Checkpoint。Join 节点不重复执行，后继节点恰好执行一次。测试还检查已发布
Join Head 与基础 Checkpoint 在结果消费期间保持不变。

## 注入边界与证据

仅测试使用的代理只接受一个明文 PostgreSQL 3.0 连接，拒绝非 Loopback 目标，
仅在正式 Pending Result INSERT 的 Parse 前缀出现后武装，并切断下一条精确的
COMMIT 交换。代理不记录协议 Frame，固定 SQLx 0.8.6 的事务封包行为。目标缺失、
Worker 提前退出、证据不足或异常协议 Frame 均直接失败。控制器只结束自己创建的
Worker，并等待对应数据库后端 PID 消失。这是客户端进程故障测试，不是 PostgreSQL
Server/WAL 故障测试。

使用可丢弃的本机 PostgreSQL 16 或 17 数据库复现：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::join::consumption_commit_loss::child_join_consumption_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

两个强制 PostgreSQL CI Job 都要求恰好两条
`STATEKNOT_CHILD_JOIN_CONSUMPTION_COMMIT_LOSS_EVIDENCE` 记录并成功退出。CI
将验收日志及宿主/源码清单保留 30 天。证据不包含数据库凭证或 Agent 输入输出。

## 仍未覆盖的门禁

不确定 Provider 副作用/计价、Failover、PITR/恢复、不可信 Worker SQL 隔离、
同 Run 嵌套命名空间、历史保留容量、延迟和 Soak 仍未验收。RFC-0004 继续为 Draft；
本次验收不代表完整可恢复子 Run 能力已生产就绪。
