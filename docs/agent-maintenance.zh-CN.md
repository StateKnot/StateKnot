<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 独立 Agent 维护角色

`stateknot::agent_maintenance`（也由 `stateknot-runtime` 导出）管理已有的四类
维护任务，独立于 HTTP 接入和执行 Worker。API 仍为实验性，框架仍处于 Pre-alpha，
不等于稳定版本或完整多角色生产部署验证。参见
[RFC-0014](rfcs/0014-owned-agent-maintenance.md) 与
[完整英文 API 示例](agent-maintenance.md#bind-actual-dependencies)。

## 启动绑定

宿主先授权 1–128 个不重复租户，注册截止时间、子 Run 协调、Join、失败收敛四份
标准审计 Schema，再冻结注册表。通过 `AgentMaintenanceBinding::new` 绑定同一
实际 Store、冻结 Schema、租户列表和 `AgentMaintenanceMutationOptions`；
通过 `AgentMaintenance::start(binding, dependencies, options).await` 启动。
四个具体维护器全部存在，内部句柄不外泄，不接受替代真实维护逻辑的任意回调。

构造时检查租户和精确离线 Schema；启动时检查实际数据库 Schema 与强制
`AgentMaintenanceReadiness`，成功前不启动维护任务、不修改业务记录。宿主检查
必须只读、异步、可取消，反映实际凭据、租户配置及依赖可用性；常量返回成功不能
代替生产资格验证。使用并验证[受限 SQL 角色](postgresql-roles.zh-CN.md)。没有
通配租户发现或隐式授权，数据库仍是可信服务端边界，不是非可信 Worker 的隔离沙箱。

## 四条维护路径

| 任务 | 具体实现 | 每个 Tick 的上限 |
| --- | --- | --- |
| Deadline | `DurableAgentDeadlineReconciler` | 16 个已到期 Admission |
| Child | `DurableChildReconciler` | 16 条取消传播 + 16 条结算 |
| Join | `DurableChildJoinPublisher` | 16 个符合发布条件的父 Run |
| FailureClose | `DurableRunFailureCloser` | 16 个失败收敛候选 |

同时只运行一个 Tick，按排序后的租户及上述任务顺序轮询。若没有停机或就绪中断，
每个“租户 × 任务”在 4N 个已接收 Tick 内获得一次机会。这是单实例轮询公平性，
不是跨实例加权预留或时间 SLA。多副本依赖已有数据库事务、串行化与幂等语义。
每项修改沿用现有有界重试策略；不会在本地建立无界任务队列。

每个租户、任务独立保留游标。单条失败计数后继续扫描并退避，不会让首页坏数据
堵住后续页；扫描耗尽会重置，之后重访失败记录。发现候选时的数据库错误会停止
角色，重启从空游标读取持久化状态，不保存会永久跳过失败记录的高水位。
截止时间路径只发起取消，执行 Worker 仍负责真实清理和生命周期确认，不能伪造完成证据。

## 时限、健康状态与停机

默认正常 Tick 间隔 250 ms，条目失败后 1 s；单 Tick 绝对时限 30 s，优雅排空
10 s。单飞就绪检查在上次完成后间隔 10 s，整体时限 5 s，证据有效期 20 s。
通过 `with_pacing`、`with_deadlines`、`with_readiness_limits` 在有限范围内调整；
租户越多，完整扫描可能越慢，必须按真实负载测量积压和处理延迟。

`role.health()` 提供 Ready / Unavailable / Draining / Stopped、`active_ticks()`
和脱敏 `report()`。Ready 只表示依赖证据新鲜，不表示条目全部成功。
`report.job(AgentMaintenanceJob::Deadline)` 等返回每类任务的 Tick、候选和失败
计数；可能包含重复观察，饱和累加并在进程重启后归零，不是持久化计费或使用量。
健康数据不含 SQL、凭据、租户/Run 标识或正文，不自动开放 HTTP 端点。即使 Ready，
条目错误持续增加也必须告警；通过受保护的底层接口定位具体失败记录。

就绪失败或过期暂停新 Tick，已接收的工作可完成。候选发现错误、任务 Panic、
Tick 超时会记录首个封闭错误类别并排空。Panic Hook 的敏感信息处理由宿主负责。
`role.begin_shutdown()` 同步关闭新工作接收；`role.shutdown().await` 等待两个
内部任务结束，排空超时后 Abort 并 Join。取消 `wait(&mut self)` 不会丢失句柄。
Drop 只发起清理，不能保证同步完成；Drop 后 Stopped 可能早于被取消 Tick 销毁，
应检查活动数或优先使用等待完成的停机方法。不会关闭共享连接池。

强制取消不代表事务回滚、租约释放、外部效果撤销或 Exactly-once。提交应答丢失时
仍从既有持久化事实恢复。非让出式本机代码需要 OS 进程监督器的外层硬截止时间。
进程停机不是用户的持久化 Run 取消请求。

## 部署和验证边界

分别运行接入、执行和维护角色。通常先停止接入新请求，再按应用策略排空执行与
维护，保留其他实例的恢复容量。不会隐式启动数据保留删除、Timer/Interrupt
业务策略、迁移、信号处理、凭据分发、匿名健康端点或模型/工具执行；这些仍需明确
的服务所有者。回滚使用原应用读取同一批持久化记录，不涉及 Schema 降级。

PostgreSQL 16/17 必跑资格测试覆盖四条真实路径、并发副本、范围外租户不被修改、
整页 16 条错误后仍可处理尾部、重启重访、真实 SIGKILL 与新实例恢复、原始取消及
失败身份保持、就绪丢失/恢复、Panic/数据库错误停止，以及数据库锁导致的 Tick
超时。单元测试覆盖强制 Abort/Join 和首次轮询前终止。CI 要求四类非空证据标记，
归档日志、源码 Tree、Lockfile 摘要和固定镜像；已有事务、进程恢复及权限测试仍
必须通过。这些是正确性证据，不是生产吞吐量、容量或 RTO 承诺。
执行 Worker 测试还在同一 Store 上并行运行两个角色，等待维护停机后继续提交并
完成新的 Run，验证独立停机不会关闭共享资源或中断另一角色。
