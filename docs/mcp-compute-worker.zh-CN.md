<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP 计算 Worker

`stateknot::McpComputeNode` 可以把纯 Graph 计算放到独立 MCP 服务执行，
不向 Worker 分发数据库或控制面凭证。适配器在受信任宿主里运行，Worker
只接收显式选择的状态字段；租约、取消、结果提交和记账始终由宿主负责。

这是已实现的 pre-alpha 限定能力，不代表稳定 API、任意代码沙箱、带副作用
Worker API 或生产发行版。完整边界见
[RFC-0005](rfcs/0005-mcp-compute-worker-boundary.md)。

## 使用范围

适合可重复执行的解析、归一化和纯业务计算。不允许模型调用、计费 API、
写操作、读取可变外部状态、创建子 Run 或 MCP sampling。这些工作必须使用
`DurableInvocationExecutor` 和已有模型/Tool 适配器保留耐久执行与计费证据。

宿主在启动时固定选择 `UpdateAndContinue` 或 `Terminal`。远端不能通过结果
选择路由、等待、子任务、租约、身份、用量或 invocation binding。输出 Schema
和 Graph reducer 由本地控制。Schema 合法并不证明被入侵的 Worker 计算正确。

## 绑定已有 MCP 服务

先建立固定 HTTPS 端点、已认证部署身份和专属凭证的 `McpClient`，以及离线
`JsonSchemaRegistry`。Tool 声明的输入/输出 Schema 必须与本地规范化文档完全
一致，包括 `$id` 和版本。完整 Tool 描述符的 SHA-256 必须来自独立审核的发布
清单，不能在启动时自动信任当前远端 discovery 返回的内容。

```rust
use stateknot::{
    McpComputeNode, McpComputeNodeBinding, McpComputeOutput, WorkerInputProjection,
};

let binding = McpComputeNodeBinding::new(
    &compiled_graph,
    node_id,
    worker_input_schema,
    WorkerInputProjection::new(["document", "locale"])?,
    McpComputeOutput::UpdateAndContinue,
    expected_server_revision,
    "normalize_document",
    reviewed_tool_descriptor_digest,
)?;
let executor = McpComputeNode::connect(
    authenticated_client,
    binding,
    frozen_schemas,
    std::time::Duration::from_secs(20),
).await?;
executable_builder.register_node(std::sync::Arc::new(executor))?;
```

上面是接入片段；完整 Graph、第一方 MCP Server 和恢复路径在
[`mcp_compute.rs`](../crates/stateknot/tests/mcp_compute.rs) 及其
[PostgreSQL 模块](../crates/stateknot/tests/mcp_compute/postgres.rs) 中参与编译验证。
测试 token、验证器和明文本机监听仅用于测试，不是生产认证配置。

输入采用最多 64 个顶层字段的显式白名单，禁止重复。被选字段会包含整个
有界值，因此不能选择含秘密的嵌套对象。空白名单发送 `{}`，不代表完整状态。
缺少字段会在远端调用前失败；不存在全量状态通配符或远端指定投影。

Tool 必须返回完整结果、空 `content` 和合法 `structuredContent`。文本、资源、
MRTR、通知、Tasks 和参数提升为 HTTP Header 均不在此能力内。
`readOnlyHint` 只是远端提示，不是无副作用证明。

## 部署检查单

1. Worker 使用独立 OS/工作负载身份、文件和网络策略；不能拥有 PostgreSQL、
   控制面或模型供应商凭证，也不能挂载共享秘密、访问云元数据或管理回调。
2. 受信任宿主先完成 Agent 提交的认证和授权。字段白名单是数据出域策略，
   必须覆盖使用该绑定的每个租户，不能依赖 Worker 自行过滤秘密。
3. HTTPS 必须验证证书。Worker 配置 MCP Bearer 验证、精确 Host/Origin 白名单、
   最小 Tool scope 和有界准入。专属凭证不得用于控制面或其他资源。复用
   [已有 MCP Server](mcp-server.zh-CN.md)，不要使用测试验证器。
4. 基础设施限制 CPU、内存、进程数和网络出口。Driver/客户端的并发和超时
   约束的是本地请求，不能替代对恶意远端进程的资源限制。
5. 固定并保留代码、描述符和 Schema 发布证据。Worker 代码、投影或策略变化
   必须使用新 Graph 版本，并排空旧部署；serverInfo 本身不是代码可信证明。
6. 代理和应用日志脱敏。监控 `worker.invalid_input`、`worker.invalid_output`、
   `worker.unavailable`、`worker.timeout`、`worker.cancelled`，公开诊断不包含
   原始请求、响应或凭证。

不得暴露[受信任 SQL runtime 角色](postgresql-roles.zh-CN.md)。在同一机器
启动一个清空环境变量的进程，并不等于文件系统或网络已隔离。

## 恢复与验证

每次执行最多一次 `tools/call` HTTP 交换；禁止 OAuth challenge、协议版本和
通用请求自动重发。观察到的失败使用 `RetryAdvice::Never`。节点/Driver 超时
或取消只结束本地等待，不证明远端已停止。

宿主在提交结果前崩溃，正常节点恢复可能重新执行纯计算；已提交的 pending
result 会直接复用，即使 Worker 已停止。旧 fence 不能提交迟到结果。远端不能
上报计费用量；此模式不授权付费/供应商调用，但服务自身的资源成本仍需运维
独立核算，不代表零成本。

在一次性 PostgreSQL 16 或 17 测试实例中执行：

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
cargo test -p stateknot --test mcp_compute --locked -- --nocapture
```

同时配置测试环境变量 `STATEKNOT_TEST_DATABASE_URL`；强制测试缺少地址时会
失败。独立 Worker 子进程不继承数据库/模型凭证，运行第一方 MCP Server；
pending-result 恢复前会终止并回收该进程。不得把此测试指向业务或生产数据库。

通用带副作用 Worker/控制面 API、Worker 专用 SQL 权限、基础设施隔离认证仍是
发行阻断项；本能力不关闭这些门槛，也不代表完整耐久子 Run 已通过验收。
