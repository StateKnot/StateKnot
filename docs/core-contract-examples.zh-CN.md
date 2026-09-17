<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Core 公共合约示例

状态：已经实现的 pre-alpha 证据，对应
[RFC-0001](rfcs/0001-core-domain-and-capability-model.md) 的第 1 项验证门禁。
RFC 仍为 Draft，公共 API 尚未稳定或发布。

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
```

CI 包含单独命名的示例编译步骤。依赖边界集成测试还会读取锁定的 Cargo
Metadata，并把全部直接普通依赖和开发依赖与已审查白名单比较。新增、重命名直接
依赖或加入 Target-specific 依赖，都会让门禁失败，直到 Core Runtime-neutrality
审查被显式更新。

## 每个示例证明什么

| 示例 | 可编译合约 | 明确不作出的声明 |
| --- | --- | --- |
| [`first_agent`](../crates/stateknot-core/examples/first_agent.rs) | 构造不可变 Agent/Model Descriptor、Schema-bound 有界输入、限制性 Request Limit 与完整有限的 Resolved Budget。 | 不接纳 Run、不访问 Provider，也不执行 Graph。 |
| [`typed_tool`](../crates/stateknot-core/examples/typed_tool.rs) | 生成类型化 Input/Output Schema，Canonicalize 并固定 Digest，通过离线 Registry 校验 Rust Schema，再构造框架拥有的 `ToolAdapter`。 | 不执行 Tool Attempt，也不授权外部副作用。示例内 Registry 只用于演示；生产集成使用不可变 JSON Schema 2020-12 Registry。 |
| [`model_stream`](../crates/stateknot-core/examples/model_stream.rs) | 构造有限 Streaming Request 与 Attempt Context，检查 Model Capability，再把连续的 Started → Output → Completed Event 校验为有界 `ModelResponse`；同时编译对象安全的 `Model::stream` 入口。 | 不提供 Provider、Executor、Transport、Credential 或持久化 Attempt Ledger。 |
| [`protocol_adapter`](../crates/stateknot-core/examples/protocol_adapter.rs) | 解析封闭的外部 Request，在本地分配可信 Schema Identity，限制调用方指定的输出字节，解析 Agent 合约，并拒绝注入的 Authority Field。 | 它不是 HTTP、MCP 或 A2A Transport，也不会创建持久化 Admission。 |

## 生产集成边界

这些示例刻意停在 Core 合约构造层。生产宿主仍须认证并授权调用方，解析不可变的
Tenant-owned Descriptor 与 Schema，提交持久化 Admission，通过 Invocation Ledger
执行外部 Attempt，在 Lease/Fence 下驱动 Checkpoint，并在暴露 Result 前重新校验
Terminal Evidence。对应的已实现边界见[强类型 Agent 指南](typed-agent.zh-CN.md)、
[持久化准入指南](durable-agent-admission.zh-CN.md)和
[可恢复 Agent Loop 指南](durable-agent-loop.zh-CN.md)。

通过这些证据只关闭 RFC-0001 的第 1 项验证门禁。Canonical Fixture 完整覆盖、
Fuzzing、Compile-fail 隐私检查、历史迁移、场景映射与完整安全审查仍是验收条件。
因此 StateKnot 仍处于 pre-alpha，RFC-0001 仍为 Draft。
