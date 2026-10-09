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
cargo test -p stateknot-core --test public_type_inventory --locked
cargo test -p stateknot-core --test canonical_execution_wires --locked
cargo test -p stateknot-core --test canonical_time --locked
PROPTEST_RNG_SEED=20261008 cargo test -p stateknot-core --test canonical_values --test value_properties --test nested_json_properties --locked
cargo test -p stateknot-core -p stateknot-integrations -p stateknot --doc --locked
cargo test -p stateknot-runtime --test tool_registration --locked
```

CI 包含单独命名的示例编译步骤。依赖边界集成测试还会读取锁定的 Cargo
Metadata，并把全部直接普通依赖和开发依赖与已审查白名单比较。新增、重命名直接
依赖或加入 Target-specific 依赖，都会让门禁失败，直到 Core Runtime-neutrality
审查被显式更新。

## 封闭的兼容性 Fixture 语料库

版本化的 `catalog-v1.json` 对当前提交的全部 43 份 Core 兼容性 Fixture
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

这让现有证据语料可审查且可检测篡改。下方清单已补齐当前根导出类型的逐型基准，
目录数量本身不是覆盖率；变体组合、属性/fuzz 和历史验收仍有独立门禁。

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

[`value_properties.rs`](../crates/stateknot-core/tests/value_properties.rs) 和
[`nested_json_properties.rs`](../crates/stateknot-core/tests/nested_json_properties.rs)
提供 RFC-0001 第 3 项要求的独立模型。加上 18 个标识符属性，共 35 个属性测试，
每个运行 256 个有界生成样例。CI 固定种子便于复现，完整工作区测试同时保留普通
随机种子运行。

| 要求 | 可执行模型与保留的既有覆盖 |
| --- | --- |
| 构造边界 | 身份的 ASCII/长度文法与构造和 Serde 一致；UUID 保留全部身份位，拒绝其他所有版本和 variant 类别；版本/摘要保留完整整数和字节值；时间覆盖完整范围并拒绝越界；时长拒绝数字格式、溢出和精度丢失；币种要求大写 ASCII。Schema ID 要求规范 HTTPS，issuer 身份刻意保留精确大小写。Core 各模块已有的内容、descriptor 和调用限制属性继续执行。 |
| 规范化稳定性 | 表中每个类型经过规范解码后保持 wire 值和摘要。任意 Unicode 对象键与独立 UTF-16 排序模型一致，固定补充平面/私用区反例防止误用 Rust 字符串顺序。含 Unicode key/value 和安全整数叶子的嵌套数组/对象与独立递归规范字节模型及 SHA-256 一致。现有 JSON、Graph、Checkpoint、barrier 和恢复顺序属性继续执行。 |
| 预算算术 | 三种 count 和 Money 的加、减、乘与 checked `u64` 一致；跨币种运算失败。时长算术与非负 `i64` 一致。`budget.rs`、`budget_reservation_tests.rs` 和 `child_run_budget_tests.rs` 保留限制收窄、峰值、reservation 顺序及结算去重模型。 |
| 委托交集 | caller、grant、policy scope 与三方位集合交集一致，满足结合律且不能扩大任何参与方权限。已有两方交换律和幂等模型继续执行。 |
| 扩展限制 | 完整 map 字节、单 key 字节和条目数接受精确边界，拒绝收窄一个单位；重复条目失败。独立嵌套树模型核对紧凑字节、深度、容器条目、排除 key 的值节点、解码字符串字节与 key 字节；raw/materialized 构造均符合收窄 profile，精确边界通过、减一拒绝，尾部空白命中 raw 字节门禁。Opaque/schema-bound 扩展的构造和收紧会重新验证原先较宽的值，不能绕过逐值限制。既有插入顺序/字节会计属性与硬限制确定性测试继续执行。 |

本轮值类型证据继续保留。下面的根导出清单补齐全部当前公开序列化类型的
基准映射；剩余变体组合、嵌套属性及历史迁移验收继续开放。这些测试不构成生产容量、fuzz 资格验证或新版本发布。

[`canonical_time.rs`](../crates/stateknot-core/tests/canonical_time.rs) 还检查时间戳
20 个数字位置上的全部 2,360 种 ASCII 非数字替换，以及保持字节长度和分隔符的
Unicode 替换。直接解析、JSON 文本/值 reader 和保留的嵌套 Run transition 均拒绝
非法时间戳。所有构建 profile 都先校验数字，再执行算术；有效 wire 字节、范围和
schema pin 保持精确一致。

### 完整执行 wire 的类型映射

[`canonical_execution_wires.rs`](../crates/stateknot-core/tests/canonical_execution_wires.rs)
为另外 67 个公开类型提供逐型映射：62 个封闭对象/带标签的变体及五种身份集合。
显式 JSON pointer 将 `core-execution-wires-v1.json` 中的 104 个正向向量交给真实
公开 reader。每个向量在规范往返后保留 wire 值、RFC 8785 字节和摘要。对象向量
拒绝错误形状、未知授权字段及原始重复键；32 个显式选择的本地可验证摘要字段
额外拒绝替换和遗漏。集合拒绝错误形状、重复身份，并固定空集合是否允许。

| 公开类型 | 完整 fixture 类型族 |
| --- | --- |
| `CompiledGraph`, `GraphExecutionLimits`, `GraphNode`, `GraphReducerReference`, `GraphRoute`, `GraphRoutes`, `ReadyNodes` | `graph` |
| `CheckpointHead`, `CheckpointState`, `CheckpointWrite`, `GraphReference`, `CheckpointBarrier`, `BarrierResultHeads` | `barrier` |
| `NodeActivation`, `RunFence`, `JournalHead`, `NodeAttempt`, `NodeAttemptStart`, `NodeAttemptStartHead`, `NodeAttemptCompletion`, `NodeAttemptOutcome` | `node_attempt`, `node_result` |
| `NodeControl`, `NodeStateChange`, `NodeStateUpdate`, `NodeTerminalOutput`, `NodeWait`, `NodeWaits`, `NodeInvocationBinding`, `NodeInvocationBindings`, `PendingNodeResult`, `PendingNodeResultHead`, `PendingNodeResultIntent` | `node_result`, `durable_wait`, `model_invocation` |
| `DeliveryFence`, `OutboxDestinationRef`, `OutboxDelivery`, `OutboxDeliveryIntent`, `OutboxDeliveryHead`, `OutboxAttempt`, `OutboxAttemptStart`, `OutboxAttemptStartHead`, `OutboxAttemptCompletion`, `OutboxAttemptOutcome` | `outbox` |
| `InterruptRecord`, `InterruptRequest`, `InterruptRequestHead`, `InterruptRequestIntent`, `InterruptResolution`, `InterruptResolutionIntent`, `InterruptResolver`, `DurableTimer`, `DurableTimerHead`, `DurableTimerRecord`, `TimerFiring`, `TimerFiringIntent`, `TimerRegistrationIntent`, `WaitRegistrationIntent`, `DurableWait` | `durable_wait` |
| `ModelInvocation`, `ModelInvocationIntent`, `ModelInvocationHead`, `ModelInvocationState`, `ModelInvocationTransition` | `model_invocation` |
| `ToolInvocation`, `ToolInvocationIntent`, `ToolInvocationHead`, `ToolInvocationState`, `ToolInvocationTransition` | `tool_invocation` |

无字段的带标签变体现通过共享封闭对象 reader 拒绝额外字段，修复节点
control/state change、prepared model/tool state、Journal source/expectation
及 Pending Run state 的既定默认拒绝契约。有效 wire 字节、Rust 变体和生成的
schema pin 保持一致。此前被吞掉的字段属于非法输入；本次不重写已存记录，
也不将这类输入视作历史兼容向量。已严格解码的 RetryAdvice 保留为回归对照。

原有八个类型族测试将构造器和完整历史逐值对照到新文档，同时保留先前冻结的
摘要断言。先前 39 份文档的字节保持一致。新向量包含未完成/成功/失败的节点与
Outbox attempt、未解决/已解决的 interrupt、未触发/已触发的 timer、四种节点
control、model/tool 两种绑定、模型重试及已提交工具调用历史。

仅含引用的摘要不一定能在该类型内部独立验证：schema pin、外部目的地快照和
部分 invocation head 需要可信注册表或完整历史。矩阵只对显式选择的字段要求
本地 checksum 拒绝，并继续保留上下文相关的完整性和派发测试。本项是当前源码的
fixture 证据；N-1/N-2 迁移、剩余类型族/变体组合及完整 C2/C3 审计继续开放。

### 封闭的公开类型与 schema 清单

[`public_type_inventory.rs`](../crates/stateknot-core/tests/public_type_inventory.rs)
将全部 570 个根导出项与
[机器可读清单](../crates/stateknot-core/tests/fixtures/core-public-type-inventory-v1.json)
逐一比较：555 个类型、11 个 trait、四个常量。编译器核对 307 个同时支持
`Serialize` 与 `DeserializeOwned` 的类型、两个仅支持输出的类型，以及
246 个已审查且没有 `Serialize` 或 `DeserializeOwned` 的 Rust 类型实例。每个 reader 均有显式
fixture 文件/JSON pointer、规范 wire 摘要和生成的 JSON Schema 摘要；
`BudgetRemaining` 提供第 308 个 schema pin；独立输出清单另固定全部 308 个
序列化 profile，原输入 pin 保持精确一致。312 项矩阵检查拒绝不匹配的
标量/集合形状、181 个封闭对象向量的未知字段及原始重复已知键。
Bounded JSON 与扩展 map 保留开放 key 语义。新增导出、缺少逐型证据和意外
引入的 Serde 实现会使 CI 失败，直至完成明确审查。

[RFC-0021](rfcs/0021-core-object-readers.md) 要求全部 181 个封闭对象 reader
使用已公布的对象形状。流式 map 门禁复用原 owned 字段 reader、重复/未知字段
检查和构造器，无需额外 JSON 树。矩阵通过文本和 Value 两种 reader 拒绝按
真实声明顺序生成的数组，以及空、截短、扩展形式。Tool、Agent、Journal、
Checkpoint、子 Run 准入和 Join 的嵌套 schema reference 有直接回归；实际
Tool 注册表在应用调用前拒绝嵌套数组，合法对象仍按精确输出 pin 派发。

调用方须使用不可变 schema 指定的对象形式。以前被接受的 positional JSON
现被拒绝，不回填数据或改写 pin。这些对象支持规范 JSON 契约，不支持以
sequence 表示对象的二进制格式解码。标量、类型集合、BoundedJson 和开放扩展
保持原契约。全部 43 份 fixture、规范 wire 摘要及两组各 308 个 schema pin
保持一致；修正仅在源码中采用，不改变已发布的 alpha.1 包。

`core-admission-transcript-wires-v1.json` 补齐准入、reservation、子 Run
会计/Join、provider replay/tool outcome、Run 生命周期和组合 source 的
完整 wire。六个原有构造族逐值复现完整载荷；先前 40 份文档保留精确字节。
`BudgetRemaining` 与 `GraphNodeSource` 保持仅输出：生产者被实际检查，
未经审查加入 owned reader 会导致编译失败。泛型 Rust 类型的拒绝门禁采用
清单中明确记录的代表实例，不证明所有未来条件泛型实现都无法序列化。

当前根导出类型清单缺口已补齐。所选向量不是全部变体组合或历史版本枚举。
C2 变体审查、C3 嵌套/属性审计和 C6 真实 N-1/N-2 验收仍需分别完成；
下述有界 C4 门禁独立运行，RFC-0001 保持 Draft。

## 有界 fuzz 与输出 schema 验收

[`fuzz/qualify.py`](../fuzz/qualify.py) 运行三个实际生产边界的 ASan/libFuzzer
入口：严格有界 JSON/JCS、全部 307 个 typed reader、实际离线 runtime schema
注册表。先执行实际反序列化，再以离线输入 schema 检查被接受的输入，避免
提前验证掩盖 reader 接受的额外形状；输出 schema oracle 继续执行。
固定种子变异前逐一重放全部语料，保留失败字节和新覆盖样本，记录
源文件、依赖及编译器摘要。每个目标限制输入 128 KiB、单输入 10 秒、RSS
2,048 MiB，以及最多 10,000 次执行或 60 秒变异。runner 拥有进程组，为每轮
创建独立语料目录，拒绝源快照或锁文件变化。nightly 与开发引擎独立固定，
不改变产品 Rust 1.88 和依赖，也不通过 `cfg(fuzzing)` 禁用完整性检查。

复现步骤、畸形 Unicode/重复键/深层/超大输入及失败处理见
[fuzz 指南](../fuzz/README.md)。最终不可变源码必须通过 `Core bounded fuzz`
CI；该有限 profile 不证明穷尽覆盖、历史迁移、生产容量或独立安全验收。

[RFC-0020](rfcs/0020-core-optional-output-schemas.md) 复用真实借用型 serializer
wire，修正 `Failure`、`ToolError` 和 Capability 生命周期的可选输出 schema。
原输入 schema/wire pin 保持精确一致，全部 308 个输出 pin 独立固定。真实
父提交生成的旧输出 schema 验证了 Tool adapter 的启动拒绝且零应用调用。
输出字节变化需要新 schema/Tool 版本，已准入工作保留旧可执行版本及注册表；
不能重写持久 pin。

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
