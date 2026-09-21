<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 注册本地 Rust Tool

[English](local-tools.md)

本地 Tool 是运行在 StateKnot 强类型、不可变、持久执行边界后的普通应用代码。生产接入路径为：

1. 为一个确定的行为版本实现 `stateknot_core::Tool`；
2. 在 `JsonSchemaRegistryBuilder` 中生成并固定输入、输出 Schema；
3. 构造精确的 `ToolDescriptor` 与 `ToolAdapter`；
4. 将适配器冻结到 `ToolProviderRegistryBuilder`；
5. 通过 Agent 的 `AgentTools` 暴露同一个 Descriptor。

运行完整的启动示例：

```console
cargo run -p stateknot-runtime --example local_tool_registration --locked
```

[示例源码](../crates/stateknot-runtime/examples/local_tool_registration.rs)
不会调用 Tool，也不会执行外部 I/O。它会证明 Rust 生成的 Schema、Descriptor、
可执行适配器和 Agent 可见声明都绑定到同一个不可变版本。

## 定义封闭的强类型边界

```rust,ignore
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncidentLookup {
    incident_id: String,
}

#[derive(JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct IncidentSummary {
    found: bool,
    severity: String,
}

impl Tool for IncidentLookupTool {
    type Input = IncidentLookup;
    type Output = IncidentSummary;

    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(
        &self,
        context: ToolContext,
        input: Self::Input,
    ) -> BoxFuture<'_, Result<ToolOutput<Self::Output>, ToolError>> {
        // 一次已准入的物理尝试。通过 context 获取持久调用/尝试身份、取消、
        // 截止时间和证据上下文。
        # todo!()
    }
}
```

输入应以对象为根，使用封闭的 Serde 解码、受限字段和明确的输出。不要为了省略契约
而接受无边界的 `serde_json::Value`。`ToolAdapter` 会在启动时校验生成的输入
Schema，在应用代码执行前校验有界输入，将强类型结果序列化后，再使用同一个冻结
Schema 注册表校验输出。

`call` 只代表一次物理尝试，内部不能隐藏重试。每次外部交换都必须拥有独立准入的持久
尝试证据。返回 `ToolError` 时必须如实报告副作用证据：写请求可能到达目标后，不能将
结果描述为“确定未执行”。实现应观察 `ToolContext` 的协作取消和截止时间，但 Future
被取消不表示已经发出的外部写入被撤回。

## 在启动时生成并固定 Schema

```rust,ignore
let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
let input = schemas.register_rust_type::<IncidentLookup>(
    "https://schemas.example.com/tools/lookup/input/1.0.0".parse()?,
    Version::new(1, 0, 0),
)?;
let output = schemas.register_rust_type::<IncidentSummary>(
    "https://schemas.example.com/tools/lookup/output/1.0.0".parse()?,
    Version::new(1, 0, 0),
)?;
let schemas = schemas.build()?;
```

`register_rust_type` 写入规范 `$id`、计算 RFC 8785 SHA-256 Digest、返回完整
`SchemaReference`，并注册离线 JSON Schema 2020-12 Validator。
`ToolAdapter::new` 会重新生成 Rust Schema，并要求其规范字节完全相等。过期
Descriptor、变化后的 Rust 类型、缺失的本地资源、未解析 `$ref` 或 Digest 漂移都会
在进程快照构建阶段失败，不会等到 Run 派发后才暴露。

这些 Schema URI 和版本应被当作已经发布的接口。若源码重构后生成的 Schema 字节完全
一致，可以保留版本；只要 Wire Shape 或行为发生变化，就应发布新的 Schema 与 Tool
版本。

## 准确声明副作用和资源上限

Descriptor 不是展示信息，它参与准入、调度、恢复和策略判断。必须声明：

- 精确的 Owner、注册表内名称与语义版本；
- 上一步得到的输入、输出 `SchemaReference`；
- `ToolRisk`、幂等性、重放及状态查询语义；
- 取消与进度能力；
- 有限的时间、并发、输入、输出、进度与 Artifact 上限。

若 Descriptor 声明可对账，适配器必须能查询权威 Provider 状态，或由单独授权的人工
对账器完成。不要为了并行调度而把非幂等写操作伪装成只读操作。资源需求只是策略输入，
不是权限授予。

## 同时冻结可执行视图与 Agent 视图

```rust,ignore
let descriptor = build_descriptor(input, output)?;
let adapter = ToolAdapter::new(
    IncidentLookupTool { descriptor: descriptor.clone() },
    schemas.clone(),
)?;

let mut providers = ToolProviderRegistryBuilder::new();
providers.register(Arc::new(adapter))?;
let providers = providers.build();

let agent_tools = AgentTools::try_new([descriptor.clone()])?;
assert_eq!(providers.resolve(&descriptor)?.descriptor(), &descriptor);
```

`AgentTools` 是规范化、对模型可见的声明；Provider Registry 是 Worker 的可执行快照。
注册会拒绝重复的精确身份和不一致的对账能力。解析同时要求 Owner/Name/Version 相同，
且 Descriptor 字节完全一致；恢复期间不存在别名、优先级或 Fallback 选择。

启用、停用或升级 Tool 时，应构建新的不可变部署快照，不能修改正在使用的 Registry。
所有持久引用旧版本的在途 Run 排空前，必须保留旧的可执行版本。

## 让持久执行留在 Tool 代码之外

Agent Node 不应直接调用适配器。生产顺序由 `DurableInvocationExecutor` 负责：

1. 校验预算、策略、Descriptor 与有界输入；
2. 将精确的 Attempt Start 提交到 PostgreSQL；
3. 从不可变 Registry 解析精确 Descriptor；
4. 不持有数据库事务地执行一次 Provider 调用；
5. 使用 Fence 提交终态结果或失败证据。

如果进程在派发后、终态提交前退出，恢复逻辑会使用 Descriptor 声明的幂等与对账语义。
未知副作用不能被自动转换成重复写入。Handoff、对账和响应丢失语义见
[持久模型与 Tool 调用执行](durable-invocation-executor.zh-CN.md)。

## 本地、MCP 与 A2A Provider 可以并存，但没有覆盖优先级

本地 `ToolAdapter`、`McpRemoteTool`、`McpSkillBoundTool` 和
`A2aRemoteAgent` 都实现 `ErasedTool`，可以作为 `Arc<dyn ErasedTool>` 注册到同一个
`ToolProviderRegistryBuilder`。协议只改变传输行为，不改变持久身份和执行规则。

精确身份冲突会导致启动失败。StateKnot 不允许远端发现结果静默覆盖本地代码，也不会在
本地版本缺失时退回到同名远端能力。必须在 Agent 准入前完成发现、策略和版本选择，然后
冻结结果。远端 Skill 的专用审批应使用
[MCP Skills Client 与 Host](mcp-skills-host.zh-CN.md) 的受保护 Profile。

## 生产检查清单

- 每个可执行行为只绑定一组不可变 Descriptor 与 Schema。
- 拒绝未知输入字段，并为所有可能增长的值设置边界。
- 如实声明副作用、幂等性、取消和对账能力。
- 凭证与客户端由 Tool 实现持有；Secret 不能进入 Descriptor、Error、Event 或模型可见输出。
- 一次 `call` 只执行一次物理尝试，由持久 Runtime 管理重试。
- 在同一 Worker 快照中注册所有 Agent 可见 Descriptor。
- 在宣称生产资格前，使用真实依赖验证进程退出、响应丢失、取消和 Lease 接管。
