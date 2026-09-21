<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 在进程内运行强类型 Agent

状态：已有实现支撑的预发布 API。它适合单应用进程或仍要求真实
PostgreSQL 持久化的开发部署。它不是内存执行器，也不会绕过鉴权、准入、
Graph Driver、带 Fence 的调用 Attempt 或维护任务。

[English](in-process-agent.md)

## 便捷层实际拥有的职责

`InProcessAgentRuntime` 省略的是 HTTP Listener，不是可恢复运行时。它会拥有并
监督两个角色：

- 一个 Tenant-scoped `AgentWorker`，负责可恢复调度与执行；
- 一个 `AgentMaintenance`，负责 Deadline、子 Run 取消/结算、Join 发布与
  Failure Close。

Maintenance 先启动；只有两个真实 Readiness Check 都通过后才会启动 Worker。
Maintenance 不可用时 Worker 暂停领取工作。任一角色意外退出都会触发另一个
角色 Fail-stop Drain。`shutdown().await` 先排空 Worker，再关闭 Maintenance，
并返回两者的 Join Report。直接 Drop 只会发起清理，不能证明清理完成。

Binding 仍然要求生产 Host 使用的已验证 `PostgresStore`、冻结的
`ExecutableGraphRegistry`、`AgentServiceRegistry`、Authorizer、Lifecycle
Evidence Provider、Tenant 与有限 Role Option。它不会创建测试身份、安装
Allow-all Policy、隐式执行数据库迁移，也不会回退到进程内存。

## 提交一条可恢复的强类型请求

先按照[强类型 Agent 指南](typed-agent.zh-CN.md)构建并绑定
`TypedAgent<I, O>`，再绑定已认证 Caller：

```rust,no_run
# use std::time::Duration;
# use stateknot::{core::{AgentSubmissionKey, BudgetLimits}, in_process_agent::*, runtime::{AgentServiceCaller, TypedAgent}};
# use serde::{Serialize, de::DeserializeOwned};
async fn execute<I, O>(
    runtime: &InProcessAgentRuntime,
    codec: TypedAgent<I, O>,
    caller: AgentServiceCaller,
    key: AgentSubmissionKey,
    input: I,
) -> Result<InProcessAgentRun<O>, InProcessAgentRunError>
where
    I: Serialize,
    O: DeserializeOwned,
{
    runtime
        .agent(codec, caller)
        .expect("Caller Tenant 已由该 Runtime 验证")
        .with_options(InProcessAgentRunOptions::new(
            Duration::from_millis(50),
            Duration::from_secs(30),
        ).expect("静态边界有效"))
        .run(InProcessAgentRequest::new(key, input, BudgetLimits::empty()))
        .await
}
```

应用必须自己保存 `AgentSubmissionKey`，并把它和业务请求放在一起。同一个 Key
配合字节等价的内容会返回原 Run；同 Key 不同内容会得到 Conflict。StateKnot
不会偷偷生成 Key，否则调用方在响应丢失后无法恢复原请求。

`run` 不会把可恢复状态压扁成一个 `O`：

| 结果 | 含义 | 调用方动作 |
| --- | --- | --- |
| `Succeeded` | Provenance、完整预算记账、固定 Output Schema 与强类型解码全部通过 | 使用 `output`；按需保留 Snapshot 用于审计 |
| `Failed` / `Cancelled` | 已提交终态且包含可公开的 Failure | 检查 Snapshot Outcome；不要随意换 Key 重试 |
| `Pending(WaitTimeout)` | 本次等待超时，但 Run 没有被取消 | 用同一 Key/内容重试，或通过 Service/HTTP 轮询 |
| `Pending(RuntimeUnavailable)` | 本地角色正在 Drain 或已经停止 | 启动替代 Runtime，再用同一 Key/内容恢复 |
| `Quarantined` | 完整性检查或运维策略禁止继续执行 | 交给运维处理，不要转发到另一套执行器 |

取消或 Drop `run` Future 不会修改持久化 Lifecycle。用户主动取消仍必须调用
`AgentServiceV1`，以保留鉴权和两阶段取消记录。

完整可编译示例位于
[`crates/stateknot/examples/in_process_agent.rs`](../crates/stateknot/examples/in_process_agent.rs)：

```bash
cargo check -p stateknot --example in_process_agent --locked
```

## 不重写 Agent，迁移到生产服务

进程内路径与 `AgentHost` 使用同一套 Descriptor、Schema Digest、Graph、
Authorizer、PostgreSQL Row、Submission Key、Worker 和 Maintenance 状态机。
所以迁移只改变所有权，不需要改写数据或 Agent：

1. 保留所有仍在使用的精确 Executable 与 Service Registry 版本；
2. 在 Loopback TLS Termination 后部署单独验证过的 `AgentHost`；
3. 停止接收本地新请求，再 Join 进程内 Runtime；
4. 通过 Agent HTTP v1 发送相同 Caller 身份、请求与已保存的 Submission Key；
5. 由生产 Worker 从 PostgreSQL 恢复全部非终态 Run。

不要误启两套 Tenant Scheduler。多副本必须经过既有数据库 Fencing 与容量验证；
这个便捷层不宣称支持多进程滚动发布、Public Ingress、Telemetry Export、备份或
Release SLO。相关生产边界见[自持有 Agent Host](agent-host.zh-CN.md)、
[PostgreSQL 配置](postgresql-configuration.zh-CN.md)与
[Host 验证](host-qualification.zh-CN.md)。
