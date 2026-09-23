<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Deadline Cancel-and-join 进程丢失验收

[English](deadline-join-process-qualification.md)

可执行配置 `deadline-cancel-join-committed-boundaries-v1` 验证父 Run 挂起等待
独立子 Join 时，由截止时间驱动的取消恢复。测试使用正式 Deadline、子任务协调、
租约 Fence 与 Agent Loop 路径，不增加测试专用恢复接口，也不宣称飞行中的 Provider
副作用能恰好停止一次。

## 已提交边界矩阵

每一行都在新的操作系统进程和新的 PostgreSQL 连接中运行。跨进程只传递不可变
`ChildRunKey`；Worker 从 PostgreSQL 重建准入、可执行图注册表、物理尝试、取消证据、
计费、Join 与生命周期状态。

| 阶段 | 强制终止前后必须保持的持久化事实 |
|---|---|
| `admission` | 父激活与物理尝试、原子子归属/准入、一笔预留和有效父 Fence。 |
| `join` | 完整 Join 成员已封存且父租约释放；没有发布或消费成功 Join 结果。 |
| `deadline` | 正式租户扫描同时观察父子继承的截止时间，并分别只提交一次 `agent.deadline.expired`；父事务同时排入子取消工作。 |
| `child_cancel` | 正式子协调器消费父队列，记录 `AlreadyRequested`，保留子级原有 Deadline 原因而不覆盖。 |
| `child_terminal` | 新 Worker 获取子租约，由真实 `DurableAgentLoop` 提交带已验证用量的取消终态。 |
| `settlement` | 子协调器恰好一次地以子终态用量替换预留，使父取消清理可调度。 |
| `takeover` | 更高父租约 Epoch 已提交；保留旧 Worker 的旧 Epoch 续租必须在继续恢复前返回 `StaleFence`。 |
| `parent_terminal` | 新 Worker 重建父注册表，由真实 Agent Loop 提交含子用量的取消终态；成功 Join 仍未发布、未消费。 |
| `replay` | Deadline、取消、结算队列均为空，生命周期、日志、计费、尝试、Checkpoint 与 Join 证据不再变化。 |

控制器在杀死每个自有 Worker 前后独立读取完整快照。Unix 必须观察信号 9
（`SIGKILL`），其他平台必须观察 `Child::kill`。进程正常退出、过滤器零匹配、错误
就绪消息、快照漂移、旧所有者返回非 `StaleFence`，或证据矩阵不完整，都会失败关闭。
共享测试框架把就绪等待限制为 90 秒、kill/reap 限制为 10 秒、就绪前输出限制为
64 KiB；这些只是测试安全界限，不是服务 SLO。

父子共享继承的有限截止时间，因此本配置要求真实租户扫描返回两个身份，而不是人为
只选父级。后续子取消投递必须幂等，并保留子级先提交的 Deadline 原因。这验证了生产
竞态，同时维持“首个原因获胜”语义。

## 复现与证据保留

使用隔离的 PostgreSQL 16 或 17 数据库：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::deadlines::process::deadline_join_survives_process_loss_and_rejects_retained_stale_worker \
  --nocapture --test-threads=1
```

两个 PostgreSQL CI Job 都要求恰好九条
`STATEKNOT_DEADLINE_JOIN_PROCESS_EVIDENCE` 记录以及成功退出。CI 保留
`deadline-join-process-postgres-<version>-<run-id>` 30 天，包含测试日志、Source/Tree
身份、Lockfile 摘要、Rust 工具链、PostgreSQL 镜像、内核及 CPU/内存/文件系统清单。
证据不包含数据库 URL、凭证、Agent 输入或子输出 Payload。

## 仍未覆盖的边界

本配置只在已知事务成功提交后终止进程，不会在 Deadline、取消、结算或终态事务的
COMMIT 进行中切断。独立的
[Join/Deadline COMMIT 丢失配置](join-deadline-commit-loss-qualification.zh-CN.md)
现已覆盖 Deadline 取消事务；独立的
[子 Agent 取消投递配置](child-cancellation-commit-loss-qualification.zh-CN.md)覆盖取消传播与真实
Wait 清理；独立的
[子 Run 结算配置](child-settlement-commit-loss-qualification.zh-CN.md)现已覆盖结算。
独立的 [父 Run 终态收口配置](parent-finalization-commit-loss-qualification.zh-CN.md)
现已覆盖收口；Join 结果消费仍未覆盖。它也不验证不确定
Provider 副作用/计价、数据库 Failover、
PITR/恢复、不可信 Worker SQL 隔离、历史保留容量、延迟或 Soak。RFC-0004 继续明确
保留这些门禁；通过之前不能把通用可恢复子 Run 宣称为生产就绪。
