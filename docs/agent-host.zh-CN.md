<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 同进程 Agent 宿主

`stateknot::agent_host::AgentHost` 统一管理真实 HTTP 接入、持久执行 Worker 和维护角色。
这是实验性 pre-alpha API，不代表稳定发布或完整部署方案；
[RFC-0015](rfcs/0015-owned-agent-host.md) 仍为 Draft。

## 绑定真实依赖

先配置实际使用的 [HTTP 服务](agent-http-server.md)、
[Worker](agent-worker.zh-CN.md) 和[维护角色](agent-maintenance.zh-CN.md)，
再通过 `AgentHostBindings::new(http, worker, maintenance)` 移交所有权。
HTTP 身份验证和资源授权仍为强制要求；没有默认放行器。
应用必须验证三者的数据库、租户、部署版本和执行注册表兼容。
不同最小权限连接池可以指向同一数据库，宿主不能仅凭连接对象证明数据库等价。

```rust,no_run
use stateknot::agent_host::*;
use tokio::net::TcpListener;

async fn serve(bindings: AgentHostBindings, dependencies: AgentHostDependencies)
    -> Result<AgentHostReport, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let mut host = AgentHost::launch(listener, bindings, dependencies,
        AgentHostOptions::default())?;
    let ready = tokio::time::timeout(std::time::Duration::from_secs(30),
        host.wait_ready()).await;
    if !matches!(ready, Ok(Ok(()))) {
        return Ok(host.shutdown().await?);
    }
    // 应用另行处理操作系统停机信号，并与 host.wait() 竞争等待。
    Ok(host.wait().await?)
}
```

`AgentHostDependencies` 必须提供三类真实依赖检查。各角色还会验证自己实际绑定的
存储和注册表；宿主检查则覆盖真实身份、资源策略、模型服务及执行证据等依赖。
回调必须只读、有界、主动让出执行权且可取消。不要另外挂载输入服务的克隆 Router，
否则该监听器不受统一入口门禁保护。服务在 `launch` 时同步独占认领；
重复认领会被拒绝，不会关闭原来的宿主。

## 启动、就绪与故障

`launch` 在异步启动之前返回拥有所有权的句柄，按“维护 → Worker → HTTP”启动。
已有持久化队列可能在 HTTP 启动之前开始执行；后续启动失败不会回滚已提交的业务。
任一阶段检查失败、超时或 panic，都会回收已启动角色并关闭共享接入服务；
重试时应重新构建服务和绑定。取消 `wait_ready` 不丢失所有权；
应用启动超时后必须显式等待 `shutdown()`。

`health()` 是不含业务载荷的内部 Rust 视图，不是匿名 HTTP 健康接口。
它保留真实角色健康句柄与首个归类故障，不持有连接池或注册表。
三个角色都有新鲜有效的就绪证据时，宿主才为 `Ready`。
每个 HTTP 请求都会检查统一状态，Worker 在接纳新 tick 前也会检查维护状态。
观察到依赖失效后拒绝新请求并暂停新执行；恢复后可重新开放。
这不能撤回已接纳工作，也不意味着跨角色健康状态是分布式原子快照。
外部故障仍需经过有时限的周期探测才能被观察到。

任一角色意外退出都会关闭入口并排空其他角色；普通连接错误不会直接终止宿主。
报告保留每个角色的强制回收计数和归类错误，不回传凭据或回调错误原文。
panic hook 与应用日志仍须自行脱敏。宿主不自动无限重启；
应先检查故障，再由经过验证的进程管理器决定重启。

## 停机与恢复

`begin_shutdown()` 同步关闭新请求入口；完整回收顺序为“HTTP → Worker → 维护”，
让维护任务在 Worker 清理期间继续处理结算和失败收敛。
`shutdown()` 和 `wait(&mut self)` 都允许取消等待后继续等待，不丢失句柄。
成功完成 joined shutdown 后，所有连接、SSE 生产任务、tick 和嵌套节点 future 均已销毁。
`Drop` 只能发起停止并中止任务，不能同步等待回收；计数可能稍后才归零。

停机期限沿用各角色已经校验的有限配置。外层停机预算必须覆盖三者之和及任务销毁时间；
不让出执行权的代码仍需操作系统强制结束期限。
进程停止不等于用户取消 Run，中断 COMMIT 也不证明事务回滚。
重启后通过原有日志、幂等标识及租约 fencing 恢复，不能盲目重发外部写操作。
宿主不会关闭共享数据库连接池。

## 验证与上线边界

PostgreSQL 16/17 验证 HTTP 提交到实际终态、独立依赖失效恢复、
HTTP 探测间隔为 60 秒时的兄弟角色门禁、各阶段启动失败/panic、可取消等待、
意外退出、有序强制回收、Drop 和 loopback 独占所有权。
固定版本的真实 TLS Keycloak 测试还验证恢复既有 Admission、节点不重复执行，
以及真实资源策略过期拒绝与恢复。CI 保存 `STATEKNOT_HOST_*_EVIDENCE`、
精确源码/tree、锁文件和镜像信息。数据库资格测试应在编译结束后串行执行。

`stknot.com` 只部署双语静态文档，不部署测试身份、默认凭据或未验证的 Agent 运行服务。
受认证保护的运维接口、跨进程滚动部署、实测容量/恢复 SLO 和其他稳定版验收仍需后续完成。
应用回滚只应选择已验证兼容的版本并保留持久化状态；本次没有 Schema 或依赖迁移。
