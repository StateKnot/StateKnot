<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Conformance 状态

本文记录 StateKnot 对官方 MCP Conformance 能声明什么、不能声明什么。它是一份证据报告，不是兼容性徽章。

## 冻结的评估输入

2026-09-02 使用以下命令刷新场景清单：

```console
npx --yes @modelcontextprotocol/conformance@0.2.0-alpha.11 list --requirements 2026-07-28
```

本次评估固定为：

- npm 包：`@modelcontextprotocol/conformance@0.2.0-alpha.11`；
- npm Integrity：
  `sha512-imPK9tx5gQsL6ZKQq4MrsyDYfSaIwpRmX6+ogjbeAXs9LGvxkBxWcY7KcS7TvwaBk/ZiVWl6b/naF4q83UwDRA==`；
- Source `gitHead`：`c321dd32035556e6769d3724a8ee97d87c3faaac`；
- 冻结 Requirement Revision：`2026-07-28`。

官方 Requirement Set 包含 69 个计分场景：37 个 Server 场景和 32 个 Client 场景。StateKnot 当前没有实现 MCP Server、OAuth Client、Roots、Prompts、Resources、MRTR、Tasks 或通用 MCP Client，因此**不声明完整 MCP Client、Server 或 SDK Tier Conformance**。

权威 Runner 与 Requirement Set 规则见[官方 MCP Conformance 仓库](https://github.com/modelcontextprotocol/conformance)，Tool 协议要求见 [MCP 2026-07-28 Tool Specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/tools.mdx)。

## 为什么严格 Tool Profile 不能记为官方通过

`McpRemoteTool` 是更窄的部署 Binding，不是官方 Requirement Set 评分的通用 MCP Client Role。注册或提交结果前，它有意强制要求：

- 精确且经过审核的本地 Input/Output Schema；
- 远端发现的 Input/Output Schema 与本地 Canonical Bytes 完全相同；
- 精确的预期 Server Implementation Name/Version；
- Complete `structuredContent`，并通过固定 Output Schema 校验；
- 每个已 Admission Attempt 只执行一次 `tools/call`，禁用 Transport Retry；
- 写入结果不确定时必须显式 Reconciliation。

官方必需的 `tools_call` Client Fixture 会故意发布一个没有 `outputSchema` 的 Tool，并返回没有 `structuredContent` 的 Text Content。通用 MCP Client 可以调用它，但 StateKnot 的严格 Binding 必须拒绝。把这种拒绝记为通过，或只为 Runner 放宽生产约束，都会错误描述双方合约。

StateKnot 也不会用 Expected-failure 文件把该差异“刷绿”。官方 Runner 明确规定：Baseline Failure 在冻结 Requirement Set 下仍然是 Failure；Baseline 用于回归控制，不会授予 Conformance。

## 已存在的可执行证据

以下测试是强制门禁，覆盖当前已实现 Profile：

```console
cargo test -p stateknot-integrations --test mcp_contract --locked
cargo test -p stateknot-integrations --test mcp_durable --locked -- --test-threads=1
```

第一套测试证明 Stateless Discovery、Protocol 与标准 Request Header、Server/Schema Pin、Attempt-scoped Authorization、有界 One-call 行为、Schema Drift 拒绝，以及 Lost Write Response 的 Reconcile-first 映射。

第二套测试同时使用真实 PostgreSQL Store 与真实 Loopback MCP Exchange。测试会在 Server 收到 `tools/call` 后暂停，并证明 Invocation 已经耐久进入 `Executing`；随后模拟 Write Response 丢失，证明状态进入 `Unknown`、重复执行不会再次 Dispatch、权威 Reconciliation 可以提交，且同一对账事件精确幂等，网络调用始终只有一次。CI 会在 PostgreSQL 16 与 17 上运行该测试。

这些是 StateKnot Profile Evidence，不是官方 MCP Requirement Set 的替代品。

## 将来声明 Conformance 的门槛

未来的通用 MCP Client 必须与 `McpRemoteTool` 分成不同 Surface，不能让更广互操作性削弱严格耐久 Binding。声明任何 Client Conformance 前，StateKnot 必须：

1. 定义并完成通用 Client Surface 的安全评审，明确它与已审核 `ToolDescriptor` Snapshot 的关系；
2. 实现所选冻结 Requirement Revision 中所有计分 Client 能力，包括 OAuth 与 Request-state 行为；否则不得声明 SDK Tier；
3. 在强制 CI 中运行精确冻结的官方 Requirement Set，不允许 Unexpected Failure，也不能使用误导性的整套 Expected-failure；
4. 将生成的 Checks、Runner Identity、Command、Platform 与 Date 作为 Release Artifact 提交；
5. 继续把严格 Remote Tool Profile Test 与 PostgreSQL Recovery Proof 作为独立发布门禁。

在这些条件满足前，准确状态是：**已经实现严格 MCP 2026-07-28 Remote Tool Profile；不声明官方完整 Client/Server Conformance**。
