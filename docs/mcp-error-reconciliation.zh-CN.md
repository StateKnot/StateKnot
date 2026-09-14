<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 带授权的 Tool 失败结果核对

[English](mcp-error-reconciliation.md) · [RFC-0007](rfcs/0007-authenticated-known-error-reconciliation.md)

`McpToolErrorReconciler` 为精确的 Unknown 写入尝试补录权威失败证据，不再次调用 Provider。
仅接受“确认未生效”或“确认已生效”，随后提交 Failed 记录。
这是尚未发布的 pre-alpha 受限实现，不代表框架已通过生产发布验收。
原有[成功结果核对入口](mcp-reconciliation.zh-CN.md)保持独立，协议不变。

## 先判断证据是否适用

- `not_applied`：原定写入未生效。不代表从未开始执行、没有费用，也不代表可以安全重试。
- `applied`：虽然结果失败，但原定写入已生效。记录失败不会撤销或补偿该副作用。
- 未知、部分生效、相互矛盾：继续保留未决状态，交给业务证据核查流程，不能猜选上述值。

宿主固定使用 `RetryAdvice::Never`、`ToolErrorPhase::Execution` 和来源
`mcp.reconciliation`，从原始冻结描述符构造来源证明。不会发起外部调用、自动重试、关闭 Run，
也不进行费用结算。后续执行仍需满足既有失败处理和计量证据要求。

## 接入可信宿主

1. 在离线 Schema 注册表登记 `McpToolErrorReconciler::audit_schema()`：引用包含其 `$id`、
   `1.0.0` 版本及 RFC 8785 规范化字节的 SHA-256。缺失或漂移会拒绝启动。
   同时提供成功入口时，应继续保留其原有 Schema。
2. 实现独立的 `McpErrorReconciliationAuthorizer`。将认证后的、带颁发者命名空间的主体映射到
   可信租户/身份；校验对精确 Run、Invocation、Attempt 的权限及权威失败/副作用证据。
   审核失败消息是否允许公开到 Run/Agent 接口，再返回绑定策略和证据摘要的
   `McpReconciliationGrant`。
3. 构造 `McpToolErrorReconciler::new(store, schemas, Arc::new(authorizer))`，将
   `definition()` 与 Handler 注册到 `McpServerToolRegistryBuilder`，使用现有带认证的
   `McpServerToolService` / `McpServerHttpService`。
4. 在专用 HTTPS 运维受众下，仅向经过审核的身份授予 `stateknot:reconcile-error`。
   `stateknot:reconcile-result` 不能补录失败，错误权限也不能补录成功。
   两者都不能放进普通 Agent/Worker 工具目录，不向远端提供 SQL 凭据或租约 fence。
5. 配置精确 Host/Origin、私有缓存策略，以及请求体、并发、速率、连接池上限。
   外层超时应容纳内部 15 秒操作时限；使用[可信 SQL 角色](postgresql-roles.zh-CN.md)，不使用超级用户。

[可编译注册与 HTTP 测试](../crates/stateknot/tests/mcp_reconciliation.rs)及
[失败核对验证](../crates/stateknot/tests/mcp_reconciliation/error.rs)展示完整接入。
其中静态令牌、证据策略和数据库传输设置仅供隔离测试，不能直接用于生产。
框架不提供默认放行策略。HTTP 状态或 Worker 自述不是权威证据：必须核对 Provider 保留的
原始操作身份、目标与尝试，并明确定义业务上的“已生效/未生效”。不可变保留策略和决策原件，
摘要本身不能证明事实为真。

## 提交不可变的证据请求

通过 `tools/call` 调用 `stateknot_reconcile_tool_error_v1`，字段严格限定为：

| 字段 | 含义 |
|---|---|
| `event_id` | 本次逻辑提交唯一的 UUIDv7，重试不得重建 |
| `run_id`、`invocation_id`、`attempt_id` | 原始目标与物理尝试，不是一次新执行 |
| `expected_revision` | 原 Unknown 版本，有符号 64 位范围内的规范十进制字符串 |
| `expected_digest` | 原记录的 `sha256:<64 位小写十六进制>` 摘要 |
| `failure_id` | 这次权威失败事件的稳定 UUIDv7 |
| `failure_category` | Core 失败类别，但不接受 `ambiguous_external_outcome` |
| `failure_code` | 最多 128 个 ASCII 字符的小写分段业务代码，以点分隔 |
| `failure_message` | 非空、已获公开批准的消息，最多 1,024 个 UTF-8 字节，不含控制字符 |
| `external_effect` | 只能是 `not_applied` 或 `applied` |

额外字段即使为 null 也拒绝，包括租户、fence、重试建议、阶段、来源、来源证明、输出、附件、
用量、详细诊断和恢复句柄。类型解码层还会验证 UUIDv7、摘要与数值边界。
来源和重试语义由宿主决定；文本形状校验不等于脱敏，禁止提交凭据、堆栈或私有 Provider 报文。

提交成功时，MCP `content` 为空，`structuredContent` 只有 `event_id`、`invocation_digest`
和字符串 `revision`。这里的成功指“证据已记录”，不是原 Tool 执行成功。
Failed 记录和 `mcp-tool-error-reconciled` 审计原子提交，审计 Schema 为
`https://stknot.com/schemas/runtime/mcp-tool-error-reconciliation/1.0.0`。
审计只保存可信身份/策略及摘要，不复制失败原文；公开失败消息仍会进入调用账本，应保护存储和备份。

## 回包丢失、竞争和故障处理

保留完整请求，尤其是事件 ID、失败 ID、原版本和消息。规范化摘要绑定工具名、认证主体、映射身份
及全部请求字段；JSON 键顺序和空白不影响身份。刷新令牌后，可信主体/租户/身份必须保持相同。

| 结果 | 操作 |
|---|---|
| `reconciliation.denied` | 停止，修正当前权限或证据；授权前不查询数据库 |
| `reconciliation.invalid` | 修正合约或证据问题，不能重放业务写入 |
| `reconciliation.conflict` | 在授权宿主侧检查目标、证据或先提交结果是否不同 |
| `reconciliation.busy` | 退避，不强制抢占存活 Worker |
| `reconciliation.unavailable`、超时、断连 | 可能已经提交，仅以相同请求做有界退避重试 |

设置重试次数和总时限，耗尽后交给运维。不能换事件 ID 或失败 ID 绕过冲突。
成功与失败请求竞争同一 Unknown 记录时，只允许一方成为最终事实；即使使用同一事件 ID，
另一入口仍会冲突。每次重复提交都先检查当前授权，撤销后的凭据不能读取历史回执。

新提交只申请并释放自己的租约；取消可能让租约保留至数据库时钟到期。
已提交的精确回执恢复不需要租约、不增加版本，服务重启或其他 Worker 持有后续租约时也能恢复。
Schema、证据和账本保留期应覆盖整个恢复窗口。回滚时关闭新提交，但保留兼容的回执读取能力和
错误审计 Schema；不能将既有失败记录改写为成功。

## 验证范围

在隔离的 PostgreSQL 16 或 17 数据库运行：

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 STATEKNOT_TEST_DATABASE_URL='postgres://…' \
  cargo test -p stateknot --test mcp_reconciliation --locked -- --nocapture --test-threads=1
```

CI 必须生成成功、已知失败和混合竞争三类证据标记，`mcp-reconciliation-postgres-*` 制品保存
日志及 source/tree/lock/环境信息 30 天。验证覆盖两类副作用、权限及资源拒绝、提交后回包丢失、
每类 24 路重复请求、24 路首次失败及成功/失败混合竞争、撤权、新服务恢复、过期 fence 拒绝、
Schema 漂移和成功 v1 协议兼容。测试不连接真实业务 Provider，不证明真实外部副作用。
无需迁移或依赖升级；部分生效、附件、Provider 结算/资源 fencing、通用有副作用 Worker 隔离
仍是独立验收项。
