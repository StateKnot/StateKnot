<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 子 Agent 准入 COMMIT 丢失验收

[English](child-admission-commit-loss-qualification.md)

`child-admission-commit-loss-v1` 验收正式
`PostgresStore::admit_child_run` 事务的客户端歧义 COMMIT 边界。该事务在创建
隔离子 Run、准入事件和初始检查点的同时，还必须记录归属、预留完整子预算、推进父日志，
并保持父节点执行尝试的权威性。

本配置不向生产代码加入故障钩子，不使用 SQL Trigger、事务结果 Mock 或数据库服务器崩溃。
每一格都使用新的租户、操作系统进程、单一 PostgreSQL 连接和事务。控制器只通过独立连接
读取状态，不接受被中断 Worker 的任何内存状态作为恢复证据。

## 强制矩阵

| 切断点 | 必须满足的结果 |
|---|---|
| `commit_not_forwarded` | 包括归属与预算预留在内的所有事务语句均已完成，但代理不向 PostgreSQL 转发前端 COMMIT。客户端阻塞期间，子 Run、准入、检查点、归属、预留和父日志推进都不得可见。强制终止进程并关闭其 Backend 后，状态必须与事务前完全一致；新进程随后只提交一次完整集合。 |
| `commit_response_withheld` | COMMIT 已到达 PostgreSQL，代理读取 `CommandComplete(COMMIT)` 与空闲 `ReadyForQuery` 后不向客户端转发。独立读连接此时必须看到完整原子集合。新进程使用新的父事件、子事件和检查点候选值重试，但必须返回 `Idempotent`，保留最初提交的持久化身份，不得推进任何日志或重复预留预算。 |

对比快照覆盖父 Run 的生命周期、日志头、租约、调度与检查点；候选子 Run 的准入、生命周期、
首个事件、检查点与日志；归属、祖先与派生事件；父预算账户、子身份列表和待结算工作。
恢复后，子 Run 必须仍可被认领；在子项未结算时，正式父终态保护必须拒绝完成父 Run。

## 测试插桩边界

测试专用代理只接受一个明文 PostgreSQL 3.0 会话，目标必须是 `localhost` 或字面量回环
地址。封闭的事务目标枚举只在命中精确
`INSERT INTO stateknot.child_run_ownership ` Parse 前缀后启用，并只切断下一条精确的
简单查询 `COMMIT`。启动、认证和 Bind Frame 会被转发但绝不记录；Frame 大小有上限，
部分读取状态不会丢失。

协议交换异常、目标未命中、Worker 提前退出、证据不完整或非回环目标都会失败关闭。控制器
只强制终止自己创建的 Worker，关闭对应代理会话，并等待精确的 PostgreSQL Backend PID
消失。Unix 必须观察到 `SIGKILL`。本配置固定 SQLx 0.8.6 的事务 Frame；驱动交换或目标
SQL 变化时必须重新验收。生产 TLS 和数据库流量不会经过此代理。

## 复现与证据保留

使用一次性、仅回环可达的 PostgreSQL 16 或 17 数据库：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::child_admission_commit_loss::child_admission_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

两个强制 PostgreSQL CI Job 都要求恰好两条
`STATEKNOT_CHILD_ADMISSION_COMMIT_LOSS_EVIDENCE` 记录和成功退出。CI 将
`child-admission-commit-loss-postgres-<version>-<run-id>` 保留 30 天，其中包含验收日志、
Source/Tree 身份、Lockfile 摘要、Rust 工具链、PostgreSQL 镜像、内核和宿主清单；不会
输出数据库 URL、凭证、Agent 输入或子输出。

## 仍未覆盖的门禁

这是客户端进程故障配置，不是 PostgreSQL Server/WAL 故障测试。它没有在取消投递、结算、
终态收口或 Join 结果消费的 COMMIT 进行中切断，也不验证不确定 Provider 副作用/计价、
Failover、PITR/恢复、不可信 Worker SQL 隔离、同 Run 嵌套命名空间、历史保留容量、延迟
或 Soak。因此 RFC-0004 仍为 Draft，完整可恢复子 Run 能力仍不能宣称生产就绪。
