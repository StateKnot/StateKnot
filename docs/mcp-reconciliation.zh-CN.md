<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 带授权的 MCP Tool 结果协调

[English](mcp-reconciliation.md) · [RFC-0006](rfcs/0006-authenticated-tool-result-reconciliation.md)

`McpToolReconciler` 是专用运维入口：为精确的 `Unknown` Tool 尝试补入权威的内联成功结果，
**不再次调用 Provider**。这是已实现并可验证的受限能力，不代表框架已正式生产发布，
也不是通用 Worker API 或外部写操作 exactly-once 保证。

## 宿主接入

1. 将 `McpToolReconciler::audit_schema()` 注册到离线 `JsonSchemaRegistryBuilder`。
   `SchemaReference` 使用其 `$id`、版本 `1.0.0` 和 RFC 8785 规范字节的 SHA-256。
   同时保留原 Tool 的固定输出 Schema；构建注册表时遇到缺失、漂移应启动失败。
2. 实现 `McpReconciliationAuthorizer`：把已认证、带可信签发者命名空间的 MCP subject
   映射到可信 tenant/principal；针对精确 Run、invocation、attempt 和 output 核验资源权限
   与业务权威证据。通过后才返回 `McpReconciliationGrant`。在请求之外不可变地留存它引用的
   policy 与 evidence/decision 材料。
3. 用宿主专属 `PostgresStore` 构造
   `McpToolReconciler::new(store, schemas, Arc::new(authorizer))`，以其原样 `definition()`
   注册到 `McpServerToolRegistryBuilder`，再接入现有 `McpServerToolService` 和
   `McpServerHttpService`；启用 Bearer 认证和私有缓存。
4. 生产入口必须使用校验服务端身份的 HTTPS、精确 Host/Origin 策略及专用运维 audience。
   HTTP/代理外层超时应大于 handler 的 15 秒总期限；限制速率、并发、请求大小和连接池容量。
   使用[可信宿主 SQL 角色](postgresql-roles.zh-CN.md)，不得把数据库凭据或 runtime 角色交给远端 Worker。

[可编译端到端测试](../crates/stateknot/tests/mcp_reconciliation.rs) 展示完整注册、真实 HTTP、
PostgreSQL 和恢复过程。其中固定 Token 和宽松证据策略**仅供测试**；部署必须接入真实身份系统
与业务证据核验器，没有生产默认放行实现。Scope 不证明业务写入已经成功。例如，应校验 Provider
留存的操作收据是否匹配原始目标与尝试，不能接受 Worker 的自述或实时发现元数据作为权威证据。

不要把本 Tool 放进普通 Agent/Worker 工具目录。专用 `stateknot:reconcile-result` scope
在 Tool 服务和 handler 两层检查。每次重试都在数据库查询前重新执行当前资源/证据授权，
权限已撤销的调用方不能读取历史成功收据。可信签发者命名空间和租户映射由认证器/宿主负责，
不是远端调用方可以自行声明的参数。

## 请求与收据

使用带专用凭据的现有 [MCP Client](mcp-client.zh-CN.md) 调用 `tools/call`，名称固定为
`stateknot_reconcile_tool_result_v1`。参数只有以下字段：

| 字段 | 含义 |
|---|---|
| `event_id` | 一次逻辑证据提交只生成一次 UUIDv7，重试必须保留 |
| `run_id`、`invocation_id`、`attempt_id` | 原始目标与物理 Tool 尝试，不是新执行尝试 |
| `expected_revision` | 原 Unknown 版本，规范十进制**字符串**，不超过有符号 64 位上限 |
| `expected_digest` | 对应原记录的 `sha256:<64 位小写十六进制>` 摘要 |
| `output` | 满足原本地固定 Tool 契约的内联 JSON 成功结果 |

目标应来自已授权的宿主运维记录，不提供公共枚举。未知字段拒绝；不接受 tenant、principal、
fence、schema、policy、artifact 或任意错误声明。宿主从冻结的账本描述构造结果来源，先校验再写入。

成功响应的 `content` 为空，`structuredContent` 只有 `event_id`、`invocation_digest` 和
字符串 `revision`，不返回原始结果正文。授权审计与下一条 Committed 记录在同一带 fencing 的事务中
提交。审计只记录请求/策略/决策摘要及可信 principal/policy 身份，不重复存 output 或凭据。
敏感 output 仍保存在既有调用账本中，必须保护数据库、备份及读取权限。摘要不能代替证据留存，
也不证明证据为真。

## 重试与运维处置

跨重启保留原请求值和 event ID。JSON 空白与键顺序不影响规范摘要；摘要同时绑定认证 subject
与映射后的 tenant/principal。更换 output、subject、目标或 event ID 不再是同一提交。
刷新 Token 可以，但其可信身份映射必须保持一致。

| 结果 | 处理 |
|---|---|
| `reconciliation.denied` | 停止，处理权限或证据问题；授权前不查询目标是否存在 |
| `reconciliation.invalid` | 处理固定契约不匹配；不能据此盲目重发业务写操作 |
| `reconciliation.conflict` | 在有权限的宿主侧检查目标记录，尝试/版本/event 绑定不一致 |
| `reconciliation.busy` | 有其他有效租约或有限次数日志竞争；退避后重试**同一提交** |
| `reconciliation.unavailable`、超时、HTTP 回包丢失 | 可能已经提交；有界退避后重试**同一提交**，不得换 event ID 或重新执行业务写入 |

Handler 错误是 `isError: true`、仅含固定错误码的 MCP Tool 结果。认证、协议、输入形状和入口
限流错误可能是 HTTP/JSON-RPC 错误；这些状态码都不能证明外部副作用发生与否。重试应有次数/总时限，
耗尽后升级运维处理，不应无限轮询。

不会强制抢占正在工作的 Worker。新协调才申请宿主新租约，并且只释放自己那把 fence。
取消/超时可能让租约保留到配置的数据库时钟到期时间。提交后，相同收据恢复不需要租约、
不增加日志/版本，其他 Worker 持有有效租约或服务重新创建时也能恢复；仍然必须通过当前授权。
应让 Schema、证据和账本保留周期覆盖整个恢复窗口。

## 验证与边界

在专用可丢弃数据库运行：

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 STATEKNOT_TEST_DATABASE_URL='postgres://…' \
  cargo test -p stateknot --test mcp_reconciliation --locked -- --nocapture --test-threads=1
```

PostgreSQL 16/17 必跑 CI 覆盖真实 HTTP 认证、查库前授权、带 fencing 的原子审计、提交后交付前
客户端断连、24 路并发首次提交、24 路重复读取、冲突与新服务实例恢复。
`mcp-reconciliation-postgres-*` 制品保存机器可读证据及 source/tree/lock/环境信息 30 天。
测试不调用真实业务 Provider，也不证明某笔外部业务写入确已发生。

没有数据库迁移或新增第三方版本。新增的精确版本读取验证直接前驱及日志锚点，不代替完整历史验证。
副作用已确认的失败使用[独立授权入口](mcp-error-reconciliation.zh-CN.md)。任意错误/artifact
协调、Provider 结算/资源 fencing、通用 Worker 执行、基础设施隔离、容量及故障切换
仍是独立验收项。写入发起侧参见[参数级 MCP 授权](mcp-call-authorization.zh-CN.md)。

协议参考：[MCP Tools](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/tools.mdx)
与 [MCP 安全指南](https://modelcontextprotocol.io/docs/2025-11-25/tutorials/security/security_best_practices)。
