<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 受保护的只读宿主运维接口

[English](agent-operations.md) · [RFC-0016（Draft）](rfcs/0016-protected-agent-host-operations.md)

`stateknot::agent_host::operations` 独立接管 loopback HTTP/1 监听器，读取实际
`AgentHostHealth`。业务未就绪时仍可诊断；它不执行 Agent、不探测数据库、不提供管理写操作。
StateKnot 仍为 pre-alpha，这不是稳定生产版声明。

## 三层授权必须同时成立

1. 真实 `AgentHttpAuthenticator` 验证凭据、issuer、audience、有效期与撤销状态。
   没有默认验证器，不得把测试令牌判断用在生产环境。
2. 明确授予 `AgentHttpOperation::InspectHost`。业务 Submit/Read/Cancel 不包含该权限。
   使用 `AgentHttpIntrospection` 时，调用
   `with_host_inspection_scope("stateknot:host:inspect".into())` 显式启用第四个 scope；
   它必须与三个业务 scope 不同，同时出现在令牌和可信 `TenantBinding` 授权中。
   默认三 scope 配置不能授予宿主查看权限。
3. 独立 `AgentHostOperationsPolicy` 精确列出 `AgentServiceCaller` 的租户、issuer、subject。
   这明确授权进程级运维可见性，不赋予任何 Run 数据读取或写入能力。

```rust
use std::{sync::Arc, time::Duration};
use stateknot::{agent_host::{AgentHost, operations::*},
    agent_http::AgentHttpAuthenticator, runtime::AgentServiceCaller};
use tokio::net::TcpListener;

async fn observe(
    host: &AgentHost,
    operators: Vec<AgentServiceCaller>,
    verifier: Arc<dyn AgentHttpAuthenticator>,
) -> Result<(AgentHostOperations, Arc<AgentHostOperationsPolicy>), Box<dyn std::error::Error>> {
    let policy = Arc::new(AgentHostOperationsPolicy::new(
        host.health(), operators, Duration::from_secs(300))?);
    let server = AgentHostOperations::start(
        TcpListener::bind("127.0.0.1:8081").await?, policy.clone(), verifier,
        AgentHostOperationsOptions::new(["ops.example.com".into()])?)?;
    Ok((server, policy))
}
```

策略最多 128 个不同调用方，空列表拒绝所有人；单调时钟有效期必须大于零且不超过一小时。
`generation()` 只是 CAS 版本，不是新鲜度证明。
`replace(expected_generation, callers, lease)` 验证后原子替换，支持撤销。
必须重新校验可信来源，不能给旧缓存自动续期。重复项、过时 CAS、溢出、非法上限或锁中毒
均拒绝；过期返回 503。异步身份验证和请求体校验后重新检查策略，已授权响应不追溯撤销。

## HTTP 契约

全部接口执行相同身份、权限和有效策略检查，没有匿名探针、CORS 例外、Cookie 身份或
转发头授权。TLS 由同一可信网络命名空间内的反向代理终止，保留精确 Host，限制运维网络。
不能把 backend 暴露到无保护的远端网络。

| 精确 GET 路径 | 已授权结果 |
| --- | --- |
| `/v1/host/status` | 200，包含启动、不可用、排空及已停止状态 |
| `/v1/host/live` | 未 Stopped 为 200，Stopped 为 503 |
| `/v1/host/ready` | 仅 Ready 为 200，其他为 503 |

探针的 503 仍返回状态快照，与授权失败的错误响应不同。快照包含 `schema_version: 1`、
`request_id`、`host`。host 固定字段为 `status/live/ready/failure/http/worker/maintenance`。
失败只包含封闭的阶段和角色标签；尚未启动的角色为 null。HTTP 提供状态和连接/SSE 数，
Worker 提供状态、活动 tick/节点和累计执行计数，维护提供状态、tick 及
deadline/child/join/failure_close 四项固定任务计数。
累计 `u64` 计数使用十进制**字符串**避免 JSON 精度损失；活动和强制回收数为有界整数。
计数随进程重启清零，不是持久化用量或成功结算凭证。

不会输出凭据、调用方、租户、Run ID、连接地址、SQL、回调异常或模型业务内容。
错误沿用脱敏 `agent_http.*` 格式与 `stateknot-agent` Bearer challenge：身份无效 401，
权限或独立 ACL 不足 403，身份/策略不可用 503，请求槽位满 429。
HTTP 解析器错误可能没有 JSON。拒绝非空请求体、Transfer-Encoding、Content-Encoding、
query、编码路径及重复凭据/Host/Accept；Accept 只允许缺省、`application/json`、`*/*`。
HEAD 及写方法经授权后返回 405；没有重启、取消、配置修改或清理接口。响应禁止缓存。

## 资源边界与停机

默认 16 连接、16 并发请求、5 秒请求总时限、3 秒读头、60 秒绝对连接寿命、5 秒排空。
`with_request_limits` 支持 1–256 请求、10 毫秒–10 秒；`with_transport_limits`
沿用[现有 HTTP 上限](agent-http-server.md#topology-and-bounds)。固定 64 个头、32 KiB 头缓冲、
16 KiB 响应。连接满直接关闭多余 socket；验证器超时或 panic 返回脱敏 503。
应用自己的 panic hook 也必须避免泄露秘密。回调必须让出执行权，操作系统监督器另设强杀期限。

运维 owner 与 `AgentHost` 分开保存。先停止并等待业务宿主，按需读取 Stopped 状态和报告，
最后调用 `operations.shutdown().await`。它关闭新请求与监听器，有限排空，到期中止并等待
全部连接销毁。`wait(&mut self)` 被取消后可继续等待，不丢失所有权。
检查 `AgentHttpDrainReport` 的强制回收、失败、拒绝数和监听错误，Ok 不等于全程无故障。
`Drop` 只能中止，不能同步等待回收；停止运维不会停止业务角色或关闭数据库池。

各健康字段分别同步，不是分布式原子快照；live 不证明执行器响应能力，ready 返回后也可能变化，
更不代表没有失败任务。区分身份服务 503 和已授权的非就绪快照，避免依赖故障引起重启风暴。
读取状态除了验证凭据外不做依赖 I/O，也不会自动延长授权有效期。

## 验证与后续门禁

真实 PostgreSQL 16/17 覆盖业务执行、状态独立性、权限/租户边界、过期和撤销、恶意输入、
验证器限流/时限/panic、连接上限和停机/Drop 回收。固定版本真实 TLS Keycloak 覆盖专属运维身份、
scope 显式启用、业务令牌拒绝、独立 ACL、密钥轮换、身份撤销与回收。
CI 保存 `STATEKNOT_OPERATIONS_*_EVIDENCE` 及精确源码 tree、锁文件、镜像信息。
先完成编译，再串行运行数据库资格测试。

无 Schema 或依赖迁移。新增 pre-alpha 枚举项 `InspectHost` 后，下游穷尽匹配需要更新；
已有三 scope 构造方法不变。运维 JSON 不参与持久化记录编码。
生产代理拓扑、真实凭据配置、容量与恢复 SLO、跨进程滚动上线、可观测性导出和稳定版验收仍独立进行。
`stknot.com` 仅更新双语静态文档，不部署运维监听器、测试身份或未验收的 Agent 服务。
