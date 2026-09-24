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

进程内 Runner 在准入后和每次轮询时，都会按这个精确 Submission Key 重新鉴权读取。
因此资源策略可以只授予
`RunPermission::Read` + `RunAccessTarget::Submission(key.digest_for(&tenant))`，
无需授予 `TenantRuns` 或整个 Run ID 的读取权限。返回的 Run ID 仍与准入结果
逐次比对；提交授权本身不会隐含读取授权。

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

## 运行接入 DeepSeek 的第一个 Agent

[`deepseek_agent.rs`](../crates/stateknot/examples/deepseek_agent.rs) 是可由运维配置、
可直接运行的示例，不会伪造模型回合。它组合了真实 DeepSeek Responses API、
固定摘要的输入/输出 Schema、单回合 Provider-native Graph、离线 Token 费率记账、
精确 Submission Key 的提交/读取策略、PostgreSQL，以及自持有的 Worker 和
Maintenance。只有真实模型调用及终态证据提交后，才会输出强类型答案。
[配置示例](../crates/stateknot/examples/deepseek_agent.config.json)不含密钥或数据库 URL。

1. 将配置复制到私有文件，并替换 Agent Owner/Caller 身份、问题、唯一且自行保存的
   `submission_key`、未来的 `deadline` 与 `policy_valid_until`，以及经运维核实的
   费率快照。响应丢失后必须使用同一文件和 Key 恢复。不要提交凭据或生产请求配置。
2. 按照 [PostgreSQL 配置](postgresql-configuration.zh-CN.md)准备 PostgreSQL
   16/17 的独立 Migration/Runtime 角色和已验证 TLS，通过密钥管理系统注入
   `DATABASE_URL`。仅隔离的本地数据库可显式设置 `STATEKNOT_DEV_MODE=true`
   启用不安全的 Loopback 开发模式及迁移。
3. 由进程密钥管理系统注入 `DEEPSEEK_API_KEY`，运行：

   ```bash
   cargo run -p stateknot --example deepseek_agent --locked -- /private/path/deepseek-agent.json
   ```

程序不会在生产环境隐式迁移数据库、把精确 Key 的读取授权放大成 Run ID/全租户读取，
也不会绕过持久化调用账本私自重试 Provider。同文件、同 Key 的再次执行会恢复原结果；
同 Key 不同内容会发生冲突。等待超时时 Run 仍然持久存在，进程以非零状态退出；
依赖恢复后应使用保存的原文件重试。这个有界示例不提供 HTTP Listener、工具调用、
流式输出、稳定版承诺或生产服务 SLO。

DeepSeek 官方 [Responses API 指南](https://api-docs.deepseek.com/zh-cn/guides/responses_api/)
说明了无状态 `/responses`、JSON Schema 输出与用量字段。示例把 Endpoint 固定为
`https://api.deepseek.com/`，模型固定为 `deepseek-flash`。示例费率采用发布时的
高峰价作为保守预算上界，**不是实际账单**；DeepSeek 也有低谷价，价格可能调整。
使用前请核对最新[模型与价格](https://api-docs.deepseek.com/quick_start/pricing/)，
并保留持久化重放仍需使用的全部费率版本。聊天等非密钥渠道中披露过的 API Key 应轮换。
`deepseek-flash` 是 Provider 别名，不是不可变的底层模型版本；用于生产流量前，
还需单独验证别名指向和价格变更。

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
