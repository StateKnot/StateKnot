<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Durable Child Join 进程丢失资格验证

[English](child-join-process-qualification.md)

可执行配置 `child-join-committed-boundaries-v1` 验证独立子 Run 成功路径在真实宿主
进程丢失后的恢复。它补充失败关闭与 COMMIT 丢失配置；不代表完整 Durable Child
生产配置已经启用，也不声明 Provider 外部效果 Exactly-once。

## 已提交边界矩阵

每一行都在新 OS 进程和新 PostgreSQL 连接中运行。跨进程只传递不可变
`ChildRunKey`；Worker 必须从 PostgreSQL 重新加载并校验父级准入、子级意图、
可执行 Graph、Schema、物理尝试、Accounting 与 Join 证据。

| 阶段 | 强制终止前后必须保持的持久化观察 |
|---|---|
| `admission` | 父激活、执行中的物理尝试、唯一原子子归属/准入和一个未结算预留存在；尚无 Join。 |
| `join` | 完整成员集合已封闭且父租约已释放；原物理尝试明确保持未完成。 |
| `child_terminal` | 子 Run 使用独立租约执行并达到已校验 Success；父级仍挂起且尚未消费。 |
| `settlement` | 预留被完整子树用量精确替换一次，Join 进入可发布状态。 |
| `publication` | 不可变终态/输出证据与父级唤醒 Head 已提交；发布尚未消费。 |
| `takeover` | 唤醒后提交更高父租约 Epoch；恢复继续前，保留的旧 Worker 必须收到 `StaleFence`。 |
| `parent_resume` | 新物理尝试绑定并唯一消费精确 Join Head；父 Graph 恢复并达到已校验 Success。 |
| `replay` | 新进程队列扫描为空，Lifecycle、Journal、Accounting、Attempt、Checkpoint 与 Join 证据均不变化。 |

Controller 在每个阶段独立读取完整快照，然后终止自己拥有的 Worker，并再次读取比较。
Unix 必须使用信号 9（`SIGKILL`），其他平台必须使用 `Child::kill`。Worker 正常退出、
测试过滤器没有命中、Ready 消息异常、快照变化或证据矩阵不完整都会失败。共享 Harness
把 Ready 限制为 90 秒、Kill/Reap 限制为 10 秒、Ready 前输出限制为 64 KiB；这些是
测试安全上限，不是生产 SLO。

保留 Worker 在 Join 注册前捕获原始有效 Fence，然后等待 Controller 的存活管道。
发布完成后，另一个新进程先提交严格更高的 Epoch，旧进程随后才允许写入；存储层必须
返回 `StaleFence`，不能返回 Journal 冲突或伪装成成功重放。Controller 丢失只会触发既有
孤儿 Watchdog 退出，不能计为验证成功。

## 复现与保留证据

使用隔离的 PostgreSQL 16 或 17 数据库：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::join::process::child_join_survives_process_loss_and_rejects_retained_stale_worker \
  --nocapture --test-threads=1
```

两组 PostgreSQL CI 都强制要求恰好八条
`STATEKNOT_CHILD_JOIN_PROCESS_EVIDENCE` 记录以及成功测试退出。CI 保留
`child-join-process-postgres-<version>-<run-id>` 30 天，其中包含测试日志、Source/Tree
身份、Lockfile 摘要、Rust Toolchain、PostgreSQL 镜像、Kernel 与 CPU/内存/文件系统信息。
证据不包含凭据、Agent 输入或子级输出 Payload。

## 仍未覆盖的边界

本配置只在已知提交成功后切断进程；配套的
[Deadline Cancel-and-join 配置](deadline-join-process-qualification.zh-CN.md)
验证取消分支。两者都不会在 Admission、Join、Deadline、取消、Settlement、Publication
或结果消费事务的 COMMIT 进行中终止客户端。未知 Provider 效果或计价、数据库
Failover/PITR/Restore、不受信 Worker SQL 隔离、保留历史容量、延迟和 Soak 也仍未覆盖。
RFC-0004 继续明确保留这些门禁；它们通过之前，不能把通用 Durable Child 能力宣传为
生产就绪。
