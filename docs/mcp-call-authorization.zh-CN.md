<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 耐久 MCP 写调用的参数级授权

[English](mcp-call-authorization.md)

远端工具执行写操作时，使用 `McpRemoteTool::connect_authorized`。它复用
[耐久 MCP 适配器](mcp-remote-tool.zh-CN.md)，增加完整工具描述固定和强制逐次授权。
这是可部署的受限绑定，不是通用带副作用 Graph Worker API，也不意味着 StateKnot
已发布生产版本。不能用 `McpComputeNode` 执行副作用。

## 绑定与授权

本地 Tool 描述、精确端点/服务身份、Schema 注册表和 HTTP 资源限制沿用 `connect`。
凭据参数改为 `McpToolApproval::new(reviewed_digest, authorizer)`，由可信宿主实现
`McpToolAuthorizer`：

- `resolve_startup` 只提供发现所需的最小权限凭据。
- `authorize_call` 收到不可替换的本地描述、已验证参数、端点、远端工具名、固定摘要及
  `ToolContext`。先检查租户、可信 Run 主体/同意记录、能力、实际目标参数和当前撤权策略，
  再获取本次专用凭据。返回 `PermissionDenied` 或 `Unavailable` 会阻止发送。
  没有默认放行、缓存授权、匿名回退或授权后参数改写。

`ProviderEndpoint` 可以与运维配置的端点进行精确相等比较，Debug 不显示地址。
授权请求 Debug 不包含参数、端点、租户或凭据；策略实现同样不得记录原始输入、秘密
或外部错误。这个 API 只接受可信宿主调用，不负责认证外部调用者。不能相信请求自报的
租户和 Run ID，应从宿主持有的来源记录和策略存储解析身份及同意。

部署清单保留审核过的完整原始 MCP Tool JSON，离线调用
`mcp_tool_descriptor_digest(&manifest_tool)`。启动时在 SDK 丢弃未知扩展前验证
RFC 8785 SHA-256，名称、描述、注解、Schema、元数据及扩展变化都会影响摘要。
不得用实时发现结果作为期望摘要。摘要不是可执行代码证明，远端注解也不能证明幂等或权限。
拒绝 `x-mcp-header` 参数转请求头，以及未明确禁止的 Task 执行声明。

## 执行与恢复

耐久执行器先提交物理调用开始记录，再执行授权。适配器要求存在来源事件，验证本地
绑定及参数，获取调用锁，然后在剩余截止时间内执行当前策略。只有批准的工具参数和
MCP 凭据会发往远端，不会额外携带 Run/租户/fence、预算、日志、数据库凭据或策略对象。
参数是显式数据释放边界：应用自行将秘密写入获批参数，仍然会发送该秘密。

授权拒绝、策略不可用、输入错误或策略等待超时都发生在 HTTP 调用之前，写操作保持
`ToolExternalEffect::NotStarted`。拒绝不可重试；策略临时不可用只能通过现有账本的
显式重试规则恢复。物理尝试仍然算一次尝试，不等于发生了外部写入或费用。

发送后最多一次 `tools/call`，不通过重定向、重连、认证挑战或协议版本恢复重发。
响应丢失或超时保持 `Unknown` 和 `ReconcileFirst`，不能仅凭 HTTP 状态判断未生效。
重复提交同一个耐久 handoff 只返回已保存状态，不再次授权、调用或新增账本版本。
权威结果通过现有带 fence、幂等的可信协调接口落账，不再次执行写操作；它不是供远端
匿名提交证据的接口。

发送后的失败、无效结果或 Future 被丢弃会在释放调用锁之前停用授权连接，防止 SDK
中尚未处理完的请求使用下一次尝试的新凭据。后续调用在策略执行前返回
`authorization.binding_retired`。重新建立同一审核绑定只用于新的合法工作，不能以此
重发结果不确定的写操作。发送前的策略失败不会停用连接；成功且验证通过的连接可复用，
但每个新的调用仍需重新授权。

旧 `connect` 是显式兼容路径，不会自动获得参数级授权或新连接停用保证。

## 部署与验证

数据库权限只留在可信执行器及注册表中。远端服务使用独立 OS/网络隔离及短时、限定
接收方和资源的 MCP 凭据，并在服务端独立验权、限制实际目标资源。不能将控制面或上游
Provider 凭据透传给 Worker。生产使用 HTTPS；回环 HTTP 测试不是公网部署方案。
授权之后仍可能发生撤权竞争，应限制凭据有效期并在资源端执行撤权，不能声称策略、
PostgreSQL 和远端业务之间存在原子事务。

契约测试覆盖参数/跨租户拒绝、策略不可用及超时、排队时撤权、原始扩展漂移、危险扩展、
Future 被丢弃、发送后超时、安全格式化和精确请求参数。PostgreSQL 16/17 必跑 CI
验证授权发生时开始记录已提交、拒绝落账、响应丢失、重复抑制和幂等协调。
`mcp-authorization-postgres-*` 证据保留两种结果以及精确源码、树、锁文件和环境信息，
保存 30 天。

本授权绑定不增加迁移或依赖。外部货币结算、远端资源 fencing、Provider exactly-once
副作用及通用带副作用 Worker API 仍需独立实现与验收。后续新增的
[已认证内联成功结果协调 Tool](mcp-reconciliation.zh-CN.md) 已提供受限运维入口，
不接受任意错误或 artifact；本发送侧授权能力不宣称完成上述验收项。

协议依据：[MCP 工具规范](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/tools.mdx)
及 [MCP 安全指导](https://modelcontextprotocol.io/docs/2025-11-25/tutorials/security/security_best_practices)。
