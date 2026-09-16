<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 独立运行的持久执行 Worker

状态：已有实现与验证的 Pre-alpha；[RFC-0013](rfcs/0013-owned-agent-worker.md)
仍为 Draft。这是具体执行角色，不是稳定生产版本，也不是任意代码远程执行 API。
[English](agent-worker.md)。

## 组装真实执行角色

`stateknot::agent_worker` 管理现有 PostgreSQL 租户调度器或加权公平调度器。
HTTP 接入只持久化提交，不会隐式启动 Worker。单租户使用
`AgentWorkerBinding::tenant`；跨租户使用 `::fair(...).await` 注册
`WeightedFairnessPolicy`。绑定独占一个新调度器，不可克隆，不暴露额外 Tick 入口。
公平绑定构造会注册不可变策略，但不领取 Run、不预留调度序号。宿主应限制构造耗时；
注册结果不确定时只能用相同策略重试。

```rust,no_run
use std::sync::Arc;
use stateknot::{agent_worker::*, core::TenantId, postgres::PostgresStore,
    runtime::{ExecutableGraphRegistry, GraphLifecycleEvidenceProvider}};

async fn execution_role(
    store: PostgresStore,
    registry: ExecutableGraphRegistry,
    tenant: TenantId,
    evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
    dependencies: Arc<dyn AgentWorkerReadiness>,
) -> Result<AgentWorker, Box<dyn std::error::Error>> {
    let binding = AgentWorkerBinding::tenant(store, registry, evidence, tenant,
        AgentWorkerExecutionOptions::default())?;
    Ok(AgentWorker::start(binding, dependencies,
        AgentWorkerOptions::default()).await?)
}
```

参数必须来自真实应用，框架不会生成“默认放行”的依赖。冻结注册表前安装 Driver、
Lifecycle、Cancellation 标准 Schema，以及应用 Graph、输入输出 Schema、Reducer、
节点执行器。子图还需要其现有额外 Schema 与协调任务。数据库使用已验证的
[受限运行角色](postgresql-roles.zh-CN.md)。Evidence Provider 必须从可信持久化记录
恢复准入、累计用量和产物；仅当工作确实没有计费消耗时，零用量才是真实证据。

## 边界与就绪检查

| 配置 | 默认值 | 允许范围 |
|---|---|---|
| 并发调度槽位 | 4 | 1–64 |
| 单次 Tick 绝对期限 | 5 分钟 | 100 毫秒–1 小时 |
| 优雅排空期限 | 30 秒 | 10 毫秒–5 分钟 |
| 执行后 / 空闲后 / Run 失败后间隔 | 10 / 250 / 1000 毫秒 | 各为 10 毫秒–60 秒 |
| 完成一次就绪检查后的间隔 | 10 秒 | 50 毫秒–60 秒 |
| 整次就绪检查期限 | 5 秒 | 10 毫秒–30 秒 |
| 缓存证据有效期 | 20 秒 | 不小于检查间隔＋期限，至多 120 秒 |

每个槽位最多执行一个持久调度单元；Graph 内节点并发另受 Driver 限制。
没有无界本地队列或自动扩容。Tick 期限应覆盖合法执行、数据库操作和终态记账；
超时会停止整个角色，不会自动重试可能已产生外部效果的工作。

启动及周期检查首先验证实际数据库 Schema、非空冻结注册表，以及公平调度的已存策略，
再调用强制提供的 `AgentWorkerReadiness` 检查实际 Provider、Evidence、授权和维护依赖。
同一时刻只有一次检查。失败或证据过期时暂停接纳新 Tick；已经接纳的工作可能继续完成。
后续检查成功即可恢复；检查回调崩溃则停止角色。就绪检查不是保证后续依赖永远可用的事务，
Schema 校验也不能替代数据库角色权限审计。

## 停机与恢复

1. `begin_shutdown()` 立即关闭 Tick 接纳并发出协作取消信号；不提交用户级持久取消，
   不关闭共享数据库连接池。
2. `shutdown().await` 或可安全取消等待的 `wait(&mut self)` 回收槽位与探测任务。
   优雅期限耗尽后，强制中止并等待剩余 Future 真正退出。
3. 所有 Tick 生产者退出后，再等 Graph 内受跟踪的节点 Future 完成析构。
   成功返回时本角色的活动 Tick 与节点均为零，不只是外层任务结束。
4. 用保留的同版 Graph、策略和证据绑定重启。结果不确定的数据库操作仍依靠租约、
   Fence、日志与对账恢复；不承诺已回滚、立刻可重领或外部效果恰好一次。

Drop 只能发起取消和中止，不能同步等待回收。此时健康状态可能已经是 `Stopped`，
活动计数却尚未归零。所有回调必须及时让出执行权，并在 Future 析构时释放资源。
Tokio 不能强行终止不让出执行权的原生代码，操作系统进程管理器仍须设置外层强杀期限。
执行器自行启动的框架外任务仍由执行器负责回收。

低层 Driver、Agent Loop、调度器也可获取 `GraphExecutionActivity`。
`wait_for_idle` 不是取消机制或调度锁，调用前必须停止并回收**所有**生产者，包括克隆。
独占 Worker 已在内部保证这一顺序。

## 诊断与运维职责

`health()` 提供缓存的 `Ready/Unavailable/Draining/Stopped`、活动 Tick/节点数及饱和计数。
健康句柄不保留连接池、注册表或请求内容；只能由宿主接入受保护的管理入口。
报告是进程内计数，不是持久计费、访问审计或 Run 状态。`executed_quanta` 包括等待、
延期、取消和交给维护任务的调度单元，不能当作成功 Run 数量。

Run 级 `ExecutionFailed` 计数后按有限间隔退避，现有 Agent Loop 会尝试清理精确 Fence。
调度基础设施失败、Tick 超时、任务异常崩溃则触发排空，用封闭类别报告原因。
禁止高速盲目重启：应先检查配置漂移、依赖可用性、隔离状态和待对账工作。
宿主还须配置 panic hook 与日志脱敏；捕获回调崩溃不会跳过进程已有的 panic hook，
不能把凭据或敏感请求内容写进 panic 信息。

宿主必须另行运行 Timer、Deadline、子 Run、失败收敛及保留期任务。本角色不隐式安装这些
任务、系统信号处理、迁移、凭据或 HTTP 入口。完整生产部署仍需验证停机顺序、最小权限、
资源限制、指标告警、恢复演练、实际负载与故障切换。

## 可执行证据

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL=postgres://... \
cargo test -p stateknot --test agent_worker --locked -- --nocapture --test-threads=1
```

只能使用独立测试数据库。PostgreSQL 16/17 CI 保留 `agent-worker-postgres-*` 产物，
记录源代码树、依赖锁摘要和镜像摘要。验证覆盖真实执行、就绪失效与恢复、固定并发、取消
等待、优雅及强制停机、Drop、回调崩溃、超时、Run 失败退避，以及新操作系统进程在不重复
节点的情况下完成已保留终态、跨进程延续公平调度序号。标记 ignored 的子进程测试由父测试
实际调用并强制检查；必跑 CI 缺少数据库即失败。

官网部署只发布指南，不部署测试 Worker、测试身份或公开 Agent API。完整身份与资源策略
宿主资格验证、稳定版本验收仍是后续独立工作。
