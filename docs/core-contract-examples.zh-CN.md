<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Core 公共合约示例

状态：已经实现的源码证据，对应
[RFC-0001](rfcs/0001-core-domain-and-capability-model.md) 的第 1 项验证门禁。
RFC 仍为 Draft，公共 API 尚未稳定。已发布 `0.1.0-alpha.1` 仍是预览版；
本指南跟踪当前源码证据，不表示所有增量已包含在发布包中。

四个 `stateknot-core` 示例无需 Model Provider SDK、Tokio、数据库、HTTP Server
或协议 SDK 即可编译和运行。它们调用真实的有界构造器与 Fail-closed 校验路径，
不是伪代码。

## 运行验证

```console
cargo check -p stateknot-core --examples --locked
cargo run -p stateknot-core --example first_agent --locked
cargo run -p stateknot-core --example typed_tool --locked
cargo run -p stateknot-core --example model_stream --locked
cargo run -p stateknot-core --example protocol_adapter --locked
cargo test -p stateknot-core --test dependency_boundary --locked
cargo test -p stateknot-core --test fixture_catalog --locked
PROPTEST_RNG_SEED=20261008 cargo test -p stateknot-core --test canonical_values --test value_properties --locked
cargo test -p stateknot-core -p stateknot-integrations -p stateknot --doc --locked
cargo test -p stateknot-runtime --test tool_registration --locked
```

CI 包含单独命名的示例编译步骤。依赖边界集成测试还会读取锁定的 Cargo
Metadata，并把全部直接普通依赖和开发依赖与已审查白名单比较。新增、重命名直接
依赖或加入 Target-specific 依赖，都会让门禁失败，直到 Core Runtime-neutrality
审查被显式更新。

## 封闭的兼容性 Fixture 语料库

版本化的 `catalog-v1.json` 对当前提交的全部 39 份 Core 兼容性 Fixture
文档建立封闭清单。每个条目以 SHA-256 绑定文件的精确字节，其中包括刻意无法按
RFC 8785 Canonicalize 的非法输入反例。目录根摘要则通过带 Domain Separation 的
RFC 8785 Preimage，绑定有序的路径、Schema 与内容摘要记录。

授权部分现已固定两种完整 Tool Receipt（绑定与未绑定 Skill Acting Window），
以及完整的 Skill Approval、Open Request、Acting Window 与 Revocation 记录。
可执行向量会校验每个保留摘要、Payload Redaction、封闭 Schema、严格的 Source /
Duration 解码，并验证 Identity、Input、Expiry、Reason 或 Digest 被篡改后必须
Fail Closed。

集成门禁会拒绝未登记、缺失、乱序、重复、过大、包含重复键、路径越界、Schema
冲突、内容变化或没有 Rust 测试引用的 Fixture。每份已登记文档仍必须被可执行的
Rust 兼容性测试消费；只有 Digest 不等于测试覆盖。

这让现有证据语料可审查且可检测篡改，但不代表 RFC-0001 的每个公共类型和持久化
Envelope 都已经拥有 Fixture。第 2 项验证门禁仍须完成类型级覆盖审计并补齐缺口。

### 值类型的 Fixture 与属性测试映射

[`canonical_values.rs`](../crates/stateknot-core/tests/canonical_values.rs)
将 38 个公开值类型映射到固定的正向和负向向量。每个正向值经过解码、保持原值的
序列化、RFC 8785 规范化，再从相同规范字节解码，摘要必须保持一致。测试使用真实
公开类型，覆盖全部 18 种宏生成的 UUIDv7 标识符。源码清单门禁会拒绝新增标识符
却没有接入类型化 Fixture 和属性测试的变更。

| 公开类型 | 固定 Fixture 分区 |
| --- | --- |
| `RunId`、`ThreadId`、`EventId`、`FailureId`、`MessageId`、`ArtifactId`、`AuthorizationReceiptId`、`SkillActivationApprovalId`、`SkillActingWindowId`、`InvocationId`、`InterruptId`、`TimerId`、`DeliveryId`、`DestinationId`、`CheckpointId`、`QuarantineId`、`AttemptId`、`SchedulerReservationId` | `core-identifiers-v1.json`：`uuid_v7` |
| `TenantId`、`SchedulerShardId`、`AgentSubmissionKey` | `core-identifiers-v1.json`：`tenants`、`shards`、`submission_keys` |
| `Version`、`Digest` | `core-scalars-v1.json`：`versions`、`digests` |
| `Timestamp`、`DurationMillis` | `core-time-v2.json`：`timestamps`、`durations_millis` |
| `TokenCount`、`ByteCount`、`ExecutionCount`、`CurrencyCode`、`Money` | `core-accounting-v1.json`：`counts`、`currencies`、`money` |
| `IssuerId`、`SubjectId`、`PrincipalIdentity` | `core-identity-v1.json`：`issuers`、`subjects`、`principal_identities` |
| `SchemaId`、`SchemaReference` | `core-schema-v1.json`：`ids`、`references` |
| `CapabilityName`、`Scope`、`ScopeSet` | `core-authorization-v1.json`：`capability_names`、`scopes`、`scope_sets` |

[`value_properties.rs`](../crates/stateknot-core/tests/value_properties.rs)
提供 RFC-0001 第 3 项要求的独立模型。加上 18 个标识符属性，共 31 个属性测试，
每个运行 256 个有界生成样例。CI 固定种子便于复现，完整工作区测试同时保留普通
随机种子运行。

| 要求 | 可执行模型与保留的既有覆盖 |
| --- | --- |
| 构造边界 | 身份的 ASCII/长度文法与构造和 Serde 一致；UUID 保留全部身份位，拒绝其他所有版本和 variant 类别；版本/摘要保留完整整数和字节值；时间覆盖完整范围并拒绝越界；时长拒绝数字格式、溢出和精度丢失；币种要求大写 ASCII。Schema ID 要求规范 HTTPS，issuer 身份刻意保留精确大小写。Core 各模块已有的内容、descriptor 和调用限制属性继续执行。 |
| 规范化稳定性 | 表中每个类型经过规范解码后保持 wire 值和摘要。任意 Unicode 对象键与独立 UTF-16 排序模型一致，固定补充平面/私用区反例防止误用 Rust 字符串顺序。现有 JSON、Graph、Checkpoint、barrier 和恢复顺序属性继续执行。 |
| 预算算术 | 三种 count 和 Money 的加、减、乘与 checked `u64` 一致；跨币种运算失败。时长算术与非负 `i64` 一致。`budget.rs`、`budget_reservation_tests.rs` 和 `child_run_budget_tests.rs` 保留限制收窄、峰值、reservation 顺序及结算去重模型。 |
| 委托交集 | caller、grant、policy scope 与三方位集合交集一致，满足结合律且不能扩大任何参与方权限。已有两方交换律和幂等模型继续执行。 |
| 扩展限制 | 完整 map 字节、单 key 字节和条目数接受精确边界，拒绝收窄一个单位；重复条目失败。既有插入顺序/字节会计属性及嵌套 JSON 硬限制的确定性测试继续执行。 |

本轮补齐表中值类型的缺口。内容、descriptor、错误、复合持久化 envelope、其中
嵌套标识符及历史迁移 Fixture 仍须完成 R1 全量类型审计；C2/C3 和 RFC-0001
保持开放。这些测试不构成生产容量、fuzz 资格验证或新版本发布。

## 每个示例证明什么

| 示例 | 可编译合约 | 明确不作出的声明 |
| --- | --- | --- |
| [`first_agent`](../crates/stateknot-core/examples/first_agent.rs) | 构造不可变 Agent/Model Descriptor、Schema-bound 有界输入、限制性 Request Limit 与完整有限的 Resolved Budget。 | 不接纳 Run、不访问 Provider，也不执行 Graph。 |
| [`typed_tool`](../crates/stateknot-core/examples/typed_tool.rs) | 生成类型化 Input/Output Schema，Canonicalize 并固定 Digest，通过离线 Registry 校验 Rust Schema，再构造框架拥有的 `ToolAdapter`。 | 不执行 Tool Attempt，也不授权外部副作用。示例内 Registry 只用于演示；生产集成使用不可变 JSON Schema 2020-12 Registry。 |
| [`model_stream`](../crates/stateknot-core/examples/model_stream.rs) | 构造有限 Streaming Request 与 Attempt Context，检查 Model Capability，再把连续的 Started → Output → Completed Event 校验为有界 `ModelResponse`；同时编译对象安全的 `Model::stream` 入口。 | 不提供 Provider、Executor、Transport、Credential 或持久化 Attempt Ledger。 |
| [`protocol_adapter`](../crates/stateknot-core/examples/protocol_adapter.rs) | 解析封闭的外部 Request，在本地分配可信 Schema Identity，限制调用方指定的输出字节，解析 Agent 合约，并拒绝注入的 Authority Field。 | 它不是 HTTP、MCP 或 A2A Transport，也不会创建持久化 Admission。 |

## 编译期隐私回归

Core 的 Rustdoc 分别检查 `CancellationSignal`、`ModelContext`、`ToolContext`
和 `ToolReconciliationContext` 不能满足 `serde::Serialize`。正向对照验证四个
公共类型与现有 Clone 合约，同时证明持久化 `BudgetUsage` 可满足相同的 Serde
约束。Agent HTTP 凭据文档另行验证 `AgentHttpCredential` 可构造且 Debug 脱敏，
并拒绝其序列化约束。删除导入或误加序列化实现均不能让这组证据继续通过。

其余回归覆盖 `ToolIdempotencyKey`、全部五个第一方 zeroizing 凭据包装类型
（`AgentHttpCredential`、`ClientSecret`、`ApiKey`、`A2aSecret`、
`McpServerBearerCredential`），以及静态 Provider、MCP 授权、OAuth 注册和 A2A
安全/push 凭据载体。构造对照执行公共导入和 Debug 脱敏。SDK 的
`McpOAuthStoredCredentials` 与 `McpOAuthStoredAuthorizationState` 仍允许序列化，
用于调用方拥有的加密存储；不能进入普通 Run 状态或审计 Payload。见
[OAuth 存储边界](mcp-oauth.zh-CN.md)。

两个完整 Tool 编译失败实现分别缺少输入和输出的 `JsonSchema`；正向实现补齐两个
derive。[生产注册表测试](../crates/stateknot-runtime/tests/tool_registration.rs)
验证实际方向性 Serde 输出、非法 meta-schema、非对象输入、缺失/替换 pin 和变化后的
Descriptor。拒绝派发路径断言应用调用次数为零。适配器按反序列化生成输入契约，按
序列化生成输出契约；新增输出注册入口及兼容性规则见
[RFC-0019](rfcs/0019-typed-tool-schema-directions.md) 和[本地 Tool 指南](local-tools.zh-CN.md)。

CI 包含 Rust 1.88 全工作区 Rustdoc 测试；`--all-targets` 测试与 `cargo doc` 不会
执行这些示例。这些限定检查提供当前已列出类型和 typed adapter 的 C5 证据，不能
证明任意自定义 Serde 实现，也不能关闭整个 RFC。跨阶段缺口见
[R1 验收清单](r1-contract-gap-ledger.zh-CN.md)。

## 生产集成边界

这些示例刻意停在 Core 合约构造层。生产宿主仍须认证并授权调用方，解析不可变的
Tenant-owned Descriptor 与 Schema，提交持久化 Admission，通过 Invocation Ledger
执行外部 Attempt，在 Lease/Fence 下驱动 Checkpoint，并在暴露 Result 前重新校验
Terminal Evidence。对应的已实现边界见[强类型 Agent 指南](typed-agent.zh-CN.md)、
[持久化准入指南](durable-agent-admission.zh-CN.md)和
[可恢复 Agent Loop 指南](durable-agent-loop.zh-CN.md)。

四个示例只关闭 RFC-0001 的第 1 项验证门禁。封闭 Fixture 目录只是第 2 项的
证据基础，不代表已完成所需的类型级覆盖。Fuzzing、历史迁移、场景映射与完整安全审查也仍是验收条件。StateKnot 仍是预览版，
RFC-0001 仍为 Draft。
