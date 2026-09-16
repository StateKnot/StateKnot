<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent HTTP 在线身份接入

`stateknot::agent_http::introspection::AgentHttpIntrospection` 是可直接接入
OAuth 2.0 令牌内省端点的验证器，已使用 Keycloak 26.7.3 和真实 TLS 验证。
它实现 `AgentHttpAuthenticator` 与 `AgentHttpReadiness`。这是有明确边界的
pre-alpha 接入能力，不是完整 OIDC、JWT/JWKS 本地验签、登录服务或稳定生产发行版。
[English](agent-identity.md)

## 身份可信边界与资源授权

显式配置 HTTPS 内省端点、精确签发方、资源受众、机密客户端 ID，以及提交、读取、
取消三个不同的 Scope。身份服务必须验证内省客户端，并针对本资源返回访问令牌状态。
`token_type_hint=access_token` 只是提示，不是令牌类型的安全证明。不能使用会把
刷新令牌或 ID Token 当成同一受众 Bearer 访问令牌的端点。带 `cnf` 的发送方绑定
令牌会被拒绝；本阶段不实现 DPoP/mTLS 证明。

用 `ClientSecret::new(delivered_secret)` 接收密钥管理服务下发的凭据。
私有 CA 通过 `IntrospectionOptions::with_root_certificate` 显式添加，系统公共
信任根仍然有效；没有关闭证书或主机名验证的选项。端点不得包含 URL 用户信息、
查询参数或片段；禁用跳转、环境代理、重试、内容解压、自动发现和令牌指定的地址。

有效响应必须包含匹配的 `iss`、字符串或数组形式的 `aud`、合法 `sub`、整数
`iat` 与 `exp`，以及 Bearer `token_type`。到期时间必须在未来，签发时间不能在
未来，总有效期不能超过配置上限。可选 `nbf` 必须已生效。不通过时钟宽限延长
令牌寿命，宿主与身份服务必须校时。Scope 使用 RFC 6749 语法，拒绝重复、空项和
超限列表。该实现比 RFC 7662 的可选声明基线严格，需要正确配置身份服务映射器。

`TenantBinding::new(tenant, principal, operations)` 必须来自可信控制面，不能
来自未验证声明、请求体或转发头。精确 issuer/subject 只映射一个租户；实际操作
权限为令牌 Scope 与本地授权操作的交集。未知主体和空策略不放行，没有通配身份
或匿名兜底。

**原有 `AgentServiceAuthorizer` 仍是强制边界。** 它在数据库查询前决定精确
Agent/请求、Run 或提交键的访问权，并提供原有持久化策略证据。租户映射不等于
租户内所有 Run 的读写权限，也不实现 Run 所有者 ACL；不能用它替代资源授权器
及其真实依赖就绪检查。

## 接线与策略刷新

Rustdoc 提供经过编译的构造示例。把验证器共享 `Arc` 传入
`AgentHttpService::new(service, verifier, options)`，保留同一个
`Arc<TenantPolicy>` 用于可信策略刷新，并保留验证器用于密钥更新。

`TenantPolicy::new(bindings, lease)` 从代数 1 开始。在有限的单调时钟租期到期
前，重新核验可信来源，再调用 `replace(expected_generation, fresh_bindings,
lease)`，取得下一代数。旧代数、重复主体或非法上限会使替换失败，旧快照保持不变。
失去策略来源后不能盲目续期缓存决策；空快照可明确撤销所有绑定。过期或锁中毒时
验证器返回不可用。重启必须从可信来源加载；代数只是进程内 CAS 防并发覆盖标识，
不是分布式版本或审计摘要。租期最长一小时，没有默认自动续期。

替换影响后续检查，不撤销已经授权的操作。跨副本发布与持久化审计由宿主控制面
负责。验证器不会偷偷启动刷新任务或持久化令牌、密钥。

## 就绪检查、密钥轮换与故障恢复

宿主就绪检查必须组合两个真实依赖：先调用同一验证器的 `check().await`，再检查
请求实际使用的资源授权策略。完整 Rust 接线见 [英文配套示例](agent-identity.md#readiness-rotation-and-failure-handling)。
`AgentHttpServer` 另外检查真实数据库 Schema 和可执行注册表，整个检查受统一期限约束。

身份探针先检查策略新鲜度，再用当前客户端凭据提交一个新生成的随机无效令牌，
要求身份服务返回 `active:false`，最后再次检查策略新鲜度。它不授予虚构主体，
不执行 Agent 操作。它证明 TLS、端点和当前客户端认证可用，**不能证明业务登录、
资源权限、身份服务内部正确性或未来可用性**。必须先验证提供方支持这种负向探针。

在身份服务轮换密钥后，通过密钥管理系统下发新值，再调用
`replace_client_secret`。已开始的请求可能使用旧密钥完成，没有自动重试或旧密钥
回退。若身份服务不支持凭据重叠有效期，轮换过程会出现短暂且有意拒绝放行的 503
窗口；需要协调滚动下发。验收真实轮换身份服务密钥，观察就绪下降，安装新密钥后
确认恢复。信任根、端点或客户端 ID 变更需要构造新验证器并执行正常分批发布。

无效、已撤销、过期或错误声明返回脱敏 401；有效身份缺少操作权限或资源权限返回
403；身份服务 401/5xx、畸形/超大响应、超时、验证器容量耗尽或本地策略过期返回
脱敏 503。禁止记录令牌、客户端密钥、身份服务响应体和 Authorization 头。
自有密钥包装器的 Debug 脱敏并在释放时清零，但不能保证清除 HTTP/TLS 库及分配器
内部的全部副本。

## 容量与撤销窗口

| 边界 | 默认值 / 上限 |
| --- | --- |
| 整次远端交换期限 | 3 秒 / 10 秒 |
| 并发远端交换，无等待队列 | 32 / 256 |
| 响应体 | 固定 64 KiB |
| JSON | 深度 8、单容器 128 项、共 1024 个节点 |
| 令牌总有效期 | 1 小时 / 24 小时，只允许整秒 |
| Scope | 最多 64 项，每项 128 字节 |
| 租户快照 | 最多 1024 个精确主体绑定 |
| 策略租期 | 必须显式指定，非零且不超过 1 小时 |

不缓存 active 结果。每次 HTTP 请求和 SSE 授权循环都访问身份服务。SSE 默认每秒
轮询，仍受既有队列与连接期限限制；已经进入网络缓冲区的字节无法撤回。撤销生效
速度不能强于身份服务实际报告的速度，也不等于回滚已授权操作。为所有 HTTP 请求、
事件流、副本和探针预留身份服务容量，在代理/身份服务设置全局限流。通过受保护
的宿主监控观察 503、依赖新鲜度及密钥/策略刷新失败，不可用时不能降级放行。

## 可复现验收与部署边界

`conformance/agent-identity/run.sh` 启动仅回环访问、固定镜像摘要、限制资源的
临时 Keycloak，每次生成独立测试 CA 与服务器证书，需要专用真实 PostgreSQL URL。
测试服务账号使用 client credentials，不使用密码授权模式。固定测试密钥仅用于
隔离夹具。脚本退出时清理其精确容器、匿名卷和生成的密钥，不操作其他服务。
CI 在 PostgreSQL 16/17 上执行并强制要求一个 `STATEKNOT_IDENTITY_EVIDENCE` 标记。

覆盖真实受信/不受信 TLS、提交/读取、资源策略拒绝、跨租户拒绝、权限收窄、真实
密钥轮换与就绪恢复、真实主体停用后撤销、SSE 关闭、策略过期及 HTTP 任务回收。
单元测试另覆盖恶意 JSON/响应头、声明边界、CAS、超时和取消后容量释放。这是
互操作证据，不是全部 RFC/Keycloak 认证，也不代表完整生产故障矩阵已验收。

投产前仍须分别验收本环境的 IdP 配置、密钥下发、策略源、资源授权器、TLS 代理、
SQL 角色、Worker/调度器生命周期和监控。测试 Realm、服务和账号不能用于生产。
发布官网指南不需要数据库迁移或暴露 Agent API。
[RFC-0011](rfcs/0011-agent-http-introspection.md) 在独立安全与稳定契约验收前保持 Draft。

参考：[RFC 7662](https://www.rfc-editor.org/rfc/rfc7662)、
[RFC 6749 客户端认证](https://www.rfc-editor.org/rfc/rfc6749#section-2.3.1)、
[Keycloak OIDC 端点](https://www.keycloak.org/securing-apps/oidc-layers)。
