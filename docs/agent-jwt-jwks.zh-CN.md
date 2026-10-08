<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent HTTP 本地 JWT/JWKS 身份验证

`stateknot::agent_http::jwt_jwks::AgentHttpJwtJwks` 使用运营方提供的公钥
JWKS，在本地验证 RFC 9068 访问令牌，实现 `AgentHttpAuthenticator` 和
`AgentHttpReadiness`。此功能位于当前源码，已发布的
`0.1.0-alpha.1` 不包含它。[English](agent-jwt-jwks.md)

## 配置与接入

参考可编译的 Rustdoc 示例，用 `JwtJwksOptions` 指定精确的 Issuer、
资源 Audience 与互不重复的 Submit/Read/Cancel Scope。通过
`AgentHttpJwtJwks::new(options, jwks, lease, policy)` 提供经过可信渠道
验证的公钥集和剩余有效时间。

`TenantPolicy`、`TenantBinding` 与在线身份验证共用，并从 `jwt_jwks`
重新导出。可信策略把精确 Issuer/Subject 绑定到一个租户；令牌 Scope 与
本地授权取交集。令牌中的租户字段不影响映射。独立的
`AgentServiceAuthorizer` 仍须在查询前授权具体 Agent、Run 和 Submission
Key。把验证器的共享 `Arc` 传给 `AgentHttpService::new`，并将身份就绪
检查与实际数据库、可执行注册表和资源策略的就绪检查组合。

身份服务需要为目标资源签发带 `at+jwt` 类型的 RS256 访问令牌。
必需字段为 `iss/sub/aud`、整数 `iat/exp`、非空 `client_id/jti`；
需要操作权限时携带 Scope。可选 `nbf` 必须已经生效。
验证器要求精确 Issuer、匹配 Audience、有界令牌总生命周期，且不通过
时钟偏差延长有效期。`typ=JWT` 的 ID Token、带 `cnf` 的发送方约束令牌、
加密令牌、HMAC/其他算法与 JOSE 头扩展均被拒绝。

## 公钥分发与轮换

通过校验证书的 HTTPS 或独立认证的配置通道，从指定身份服务取得公钥。
发布前确认公钥与 Issuer 的关联及获取时间。验证器不会访问令牌或密钥
元数据中的 URL。JWKS 是公开数据，但修改它会改变哪些身份能够通过验证，
因此分发通道和所有更新都需要保护与审计。

每把密钥必须包含唯一有界 `kid`、`kty=RSA`、`alg=RS256`、
`use=sig`，模数为 2048–4096 位、指数为 65537。
可选 `key_ops` 只能为 `["verify"]`。拒绝私钥字段、弱密钥和格式错误；
最多接受 16 把公钥、16 KiB 数据。可信分发流程需要把身份服务的完整集合
筛选为经过评审的 RS256 公钥集合。

调用 `replace_jwks(expected_generation, verified_jwks, remaining_lease)`
刷新。完整解析和校验成功后才原子发布；非法输入或过时版本不会替换现有
集合。`generation()` 用于进程内 CAS。计划轮换时先发布新旧公钥重叠
集合，再切换签发密钥，最后在令牌有效窗口结束后移除旧密钥。
主动发布空集合会撤销全部签名密钥，并使就绪检查失败。

只有重新确认可信来源后才能续期；从租期中扣除获取以来的时间。
把旧缓存重新发布不代表来源仍然新鲜。公钥与租户策略有独立的有效时间，
最长均为一小时。重启时从可信来源重建，在所有副本发布撤销变更。
验证器不隐式创建后台刷新任务，也不保存凭据。

## 失败语义、SSE 与容量

签名/字段无效、未知或已移除密钥返回脱敏 401；操作或资源权限不足返回
403。集合过期、验证容量用尽、锁异常或超时返回 503。
签名完成后，在解析当前租户授权前再次检查令牌时间、公钥版本和新鲜度。

SSE 每个轮询周期重新验证身份，移除密钥会关闭使用该密钥的事件流。
已经发送的字节和已经授权的操作无法追溯撤回。本地 JWT 不逐令牌查询
身份服务的撤销状态；需要即时观察服务端撤销时，使用短期令牌、明确的
租户绑定撤销，或[在线 introspection](agent-identity.zh-CN.md)。
要求 `jti` 并不等于实现了重放拒绝列表。

| 边界 | 默认值 / 上限 |
| --- | --- |
| 算法与类型 | RS256；`at+jwt` 或 `application/at+jwt` |
| Bearer 凭据 | 8192 字节 |
| 单验证器签名任务 | 32 / 64；准入不排队 |
| 验证期限 | 3 秒 / 10 秒 |
| 令牌总生命周期 | 15 分钟 / 1 小时；整数秒 |
| JWKS | 16 KiB；16 把唯一公钥 |
| 公钥有效时间 | 显式非零，最长 1 小时 |
| 租户绑定 | 最多 1024 条；独立有效时间最长 1 小时 |

RSA 验证运行于 Tokio Blocking Task，调用方取消或超时后仍运行的任务
持续占用容量直至结束。副本总限流、时钟同步及可信公钥分发由宿主部署
负责。就绪检查说明本地公钥和策略仍新鲜，不证明身份服务当前可用或
运营方提供的来源正确。禁止记录 Bearer Header、令牌正文或原始密码学错误。

## 验证证据

单元测试在内存生成两把真实测试 RSA 密钥，覆盖签名篡改、算法/ID Token
混用、必需字段、重复 JSON、危险密钥集合、CAS 竞争、过期与容量恢复。
PostgreSQL 16/17 的真实 HTTP 测试覆盖独立资源授权与跨租户拒绝、
密钥移除、SSE 关闭、过期恢复、停机回收及新验证器/监听器下的同 Key
准入恢复。证据标记为 `STATEKNOT_JWT_JWKS_EVIDENCE`。


真实 TLS Keycloak 26.7.3 门禁还验证身份服务实际签发的 RFC 9068 令牌、
可信筛选后分发的 JWKS、租户与 Scope 映射、持久化同 Key 重放和空密钥撤销。
独立测试客户端配置 `access.token.header.type.rfc9068=true`、RS256、
`client_id` 字段 Mapper 与明确资源 Audience。证据只覆盖该配置，
不泛化为所有 OIDC 提供方。配置见 `conformance/agent-identity/realm.json`。

官网发布只更新文档，不部署身份服务，也不修改数据库。应用对外开放前
需要验证自己的令牌配置、公钥分发与刷新、资源策略和多角色部署。
[RFC-0018](rfcs/0018-agent-http-jwt-jwks.md) 已接受此限定契约，框架整体生产资格
继续以[路线图](roadmap.md)为准。
