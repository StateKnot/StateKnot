<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 失败关闭的 COMMIT 丢失与租约接管验收

[English](commit-loss-qualification.md)

`failure-close-commit-loss-v1` 在[提交后进程终止配置](process-kill-qualification.zh-CN.md)
之外，增加两个精确控制的源注册事务边界。验证的是客户端在 COMMIT 调用尚未返回时
丢失，不是数据库服务端崩溃。生产 API、迁移和公开协议不变，RFC-0004 仍为 Draft。

## 故障边界与验收结果

| 截断点 | 原工作进程仍存活时的证据 | 真实强制终止后的恢复要求 |
|---|---|---|
| `commit_not_forwarded` | 源注册语句已完成，代理持有完整 COMMIT，但一个字节都未向数据库转发。独立无锁 MVCC 读取仍看到旧日志、租约、Checkpoint/账目，以及空的关闭/取消队列。 | SIGKILL 客户端，仅断开其代理会话。确认该后端会话消失后，整个原快照不变且不存在关闭证据；更高租约 epoch 可以仅注册一次。 |
| `commit_response_withheld` | COMMIT 已转发，代理收到 `CommandComplete(COMMIT)` 与空闲 `ReadyForQuery`，但两个响应均未转发。独立 Store 连接验证原始失败、直接用量和确切源事件已提交。 | SIGKILL 仍等待响应的客户端。新进程持原来已释放的 fence，用不同候选失败和零用量重试，必须恢复原始 `Existing`，不能重新注册。 |

两个场景中，新恢复进程返回经过验证的结果后也会被真正强制终止。独立维护器继续
排空子级并提交原始 Failed。源事件、结算事件和最终关闭事件各仅一条；完整日志链
通过校验，最终用量严格为父直接 7 + 子级 11 个输入 Token，终态重试不推进日志。

未转发场景还会启动一个**真正保留旧 fence 的工作进程**：加载原租约、报告就绪，
然后等待父控制进程的一次性恢复指令。控制进程通过 PostgreSQL 时钟观察租约自然
过期（测试租约 20 秒，不改写数据库时间），领取具有不同 Attempt ID、更高 epoch
的新租约，再恢复原来的旧进程。旧进程读取最新日志头，但写入必须准确返回
**`StaleFence`**，不能靠日志冲突或 Run 已关闭错误蒙混通过；持久化快照不得改变。
之后才允许另一个新进程通过有效新 fence 注册关闭。

该检查不代表所有旧 fence 写入 API、未完成图节点、未知 provider 副作用，或最终
一万次旧租约竞争的发布门槛已经验收。

## 测试装置与安全边界

`tests/postgres/commit_proxy.rs` 仅存在于测试控制端，不是公开代理或服务端功能。
只接受一个连接，目标限于 loopback 字面地址或 `localhost`；远程地址/Unix Socket
在迁移或准备测试数据前就拒绝。工作进程显式使用单连接池，连接已由控制端迁移好的
数据库。没有触发器故障钩子、SQL 异常注入、事务结果 Mock、数据库服务端终止或
后端取消请求。

装置绑定 SQLx 0.8.6 的既有交互：识别失败关闭 INSERT 的 Parse，随后捕获确切的
Simple Query `COMMIT`。仅接受明文 protocol 3.0；分配内存前验证长度（启动包
64 KiB、普通帧 4 MiB），两个方向分别保留未读完的帧状态。协议变化、截断/超长帧、
提前退出、未到达截断点或缺少响应都必须失败。帧格式依据 PostgreSQL 的
[消息格式](https://www.postgresql.org/docs/17/protocol-message-formats.html)与
[事务消息流程](https://www.postgresql.org/docs/17/protocol-flow.html)。驱动或事务
交互变化时需要重新验收；这不是通用 TLS/流水线/通知代理，不影响生产 TLS 行为。

启动、认证和 Bind 帧只转发，**不记录原文**。BackendKeyData 只保留后端 PID，不保留
取消密钥。终止所属客户端后停止并等待代理任务退出，关闭其连接，再以有界只读查询
确认该后端已断开，不终止其他会话或容器。父控制进程丢失仍由管道触发退出码 24
清理遗留工作进程；这不能算 SIGKILL 验收成功。旧进程恢复路径有独立冒烟测试，
只有执行了旧 fence 拒绝断言并由 libtest 成功退出才算通过。

到达截断点/恢复结果/恢复后退出的操作上限为 90 秒，观察后端断开为 30 秒，观察
测试租约过期为 45 秒。这些是测试装置期限，**不是服务 RTO/SLO**。

## 复现与证据留存

使用仅通过 loopback 访问、隔离且可丢弃的 PostgreSQL 16 或 17 数据库，通过环境
提供 `STATEKNOT_TEST_DATABASE_URL` 后运行：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::failure_close::commit_loss::failure_close_commit_loss_and_fence_takeover_are_recoverable \
  --nocapture --test-threads=1
```

两个强制 PostgreSQL CI 作业均运行集成测试及专门留证调用。既有
`failure-close-process-postgres-<版本>-<运行 ID>` 产物现在还包含
`failure-close-commit-loss.log`，与原六检查点日志及共享源码/tree/锁文件/工具链/
内核/CPU/内存/镜像信息一起保留 30 天。新日志必须有**按顺序排列的两条**
`STATEKNOT_COMMIT_LOSS_EVIDENCE` JSON，每个截断点一条，并且测试和作业成功退出；
空测试筛选不能过关。响应截断行的 `expired_fence_rejection_verified` 为 false，
表示该独立检查属于未转发场景，不代表漏验收。辅助工作进程入口在普通发现模式中
不是独立的真实数据库恢复测试。

## 仍未通过的生产门槛

这里仅验证直接用量已完整定价的失败关闭源事务，子级清理也来自确定测试证据。
不证明真实 provider 副作用/费用恢复，不终止正在 COMMIT/WAL 刷盘的数据库服务端，
也不代表故障切换/PITR/恢复，或所有 Admission/Join/到期/结算/最终关闭事务已验收。
SQL 角色隔离、组合运行时进程故障矩阵及长历史容量/公平性/延迟仍需完成，才能开放
完整耐久子任务生产配置。耐久账目仅记一次不等于外部副作用普遍“恰好一次”。
