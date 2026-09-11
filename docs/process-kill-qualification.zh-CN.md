<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 失败关闭的进程强制终止验收

[English](process-kill-qualification.md)

可执行配置 `failure-close-committed-boundaries-v1` 验证一条维护路径：父 Run 为
Active，直接用量完整且已定价，拥有一个子 Run，并已固定原始失败。这是
[GS-002](scenarios/002-long-running-approval.md) 的部分证据，**不等于完整耐久
子任务生产配置开放**，也不是数据库故障切换或 provider 副作用恢复认证。

## 强制终止检查点

每一行使用新 OS 进程和新 PostgreSQL 连接。维护器重建离线 Schema 注册表，从
`None` 开始扫描。跨进程只传不可变子级归属键，不传执行器、通知、游标、原始失败、
价格或记账快照。

| 已提交后的阶段 | 强制终止前后必须保持的持久化事实 |
|---|---|
| `source` | 原始失败与直接用量已封存、子级取消已入队、父租约已释放；父仍为 Active，但不能领取租约 |
| `delivery` | 唯一取消回执与公开安全的子级取消原因；送达不等于终止确认 |
| `child_terminal` | 测试子级已提交真实终态和确切定价用量；父仍未终止，子级预留尚未结算 |
| `settlement` | 子级仅结算一次，账目投影一致；父仍未终止 |
| `close` | 完整原始 Failure、父直接用量 + 子级用量、完成时间一同提交 |
| `replay` | 新扫描没有待处理项；完整日志、生命周期、回执、账目和结算不再变化 |

工作进程仅在对应公开 Store/runtime 调用返回后报告就绪。独立控制进程先读取
数据库验证已提交事实，再强制终止**自己创建的子进程**，确认异常退出并回收进程，
随后再次读取和比对证据。Unix 必须确认为真实信号 9（`SIGKILL`）；其他平台使用
`std::process::Child::kill`。丢弃 Future、关闭连接池或重建协调器不能充当进程终止。
不添加生产故障钩子、数据库迁移、公开 API 或依赖，也不终止数据库服务进程。

就绪协议使用每进程唯一令牌，读取总量限制为 64 KiB，操作超时为 90 秒。筛选到
不存在的测试、提前正常退出、非法消息、子进程模式缺少 PostgreSQL 都必须失败。
强制终止/回收的观测期限为 10 秒；断言失败时 RAII 兜底仍会终止并等待所属进程。
这些是测试装置的操作期限，不是恢复 SLO 或容量指标。装置不创建孙进程。
继承管道检测控制进程失联，令遗留工作进程以退出码 24 自行退出，并由专门测试
验证此清理路径。该退出不能作为强制终止检查点验收成功的证据。

## 运行与证据留存

仅使用**隔离、可丢弃的** PostgreSQL 16 或 17 数据库。通过环境或凭据机制提供
`STATEKNOT_TEST_DATABASE_URL`，随后运行：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::failure_close::process::failure_close_survives_os_kill_at_each_committed_drain_boundary \
  --nocapture --test-threads=1
```

测试也纳入已有的强制真实 PostgreSQL 集成套件。辅助入口
`failure_close_process_worker` 在普通测试发现中不执行场景，仅由父测试通过
命令级私有环境变量定向启动；它不是额外独立验收场景。

两个 PostgreSQL CI 作业额外运行一次可留证调用，保留
`failure-close-process-postgres-<版本>-<运行 ID>` 产物 30 天，包括源码/tree ID、
锁文件摘要、Rust 工具链、内核、CPU/内存信息和测试日志。每个成功检查点输出
`STATEKNOT_PROCESS_KILL_EVIDENCE` JSON，包含阶段、实际 PostgreSQL 版本、
OS/架构、终止方式和验证结果。只有六条记录齐全**且测试/作业成功退出**才能算通过，
部分日志不能冒充成功。正式发布证据超过 30 天需另行留存产物及不可变 PR 源码、
合并 tree 对应关系。记录不含凭据、用户提示词和私密失败诊断。

## 尚未验收的边界

测试使用确定的父/子用量（7 + 11 个输入 Token），父级直接证据已完整结清。
子级终态确认来自测试证据，不是真实 provider 的取消确认；不调用真实模型/工具，
也不证明未知外部副作用和费用能够恢复。

强制终止发生在**已知提交成功之后**，不是事务提交前、COMMIT 执行中或服务端
已提交但客户端尚未收到响应时。既有事务回滚和逻辑重试测试是独立证据，不能据此
声称已验收提交结果不明确的场景。独立的
[源 COMMIT 丢失与 fence 验收](commit-loss-qualification.zh-CN.md) 现已覆盖未转发
COMMIT、截断提交响应和保留旧进程的自然过期/接管。后续仍需其他提交前/中的 OS 终止、真实 provider
副作用和定价恢复、Join/到期/更高 fence 接管的组合进程故障、SQL 角色隔离、
故障切换/PITR/恢复、长历史容量、公平性和延迟实测。不能因此移除 RFC Draft
状态或对外宣称完整配置已经可用。
