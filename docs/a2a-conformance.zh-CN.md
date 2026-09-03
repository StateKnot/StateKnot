<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# A2A 1.0 Conformance 状态

本文记录 StateKnot 已实现的 A2A 1.0 HTTP+JSON 与 JSON-RPC Server Profile
的精确证据。它不是 Client、gRPC、Stable API 或整个框架的认证声明。

## 冻结的评估输入

可复现门禁固定了：

- 官方 TCK 仓库 `a2aproject/a2a-tck` 的提交
  `263b9cfaf16a554bdfb166a7ba5b67716e946349`；
- 下载归档 SHA-256
  `694c798e93fff30f650d44bdb3db0e1768b865a4f3ddbed64ec158db209bf5db`；
- `uv` `0.11.25`，以及在 `--frozen` 模式下读取的 TCK `uv.lock`；
- Rust `1.88.0`；
- `jsonrpc,http_json` 两个 Transport，且不使用 Expected-failures File。

门禁通过确定性 Fixture，让官方 Suite 请求真实的 `A2aServer` Router。它不会
直接调用 Contract Method，也不会绕过 Host、Body、Authentication、
Authorization、Admission、Version、Extension 或 Stream Boundary。

## 结果

| Pytest/TCK 结果 | 数量 |
| --- | ---: |
| Collected | 265 |
| Passed | 177 |
| 声明 Skip | 88 |
| Failed | 0 |
| Errors | 0 |
| Expected Failures | 0 |

TCK 的分 Surface 报告为：Agent Card `10/10`；JSON-RPC `94/101`，包含
7 个声明 Skip；HTTP+JSON `91/96`，包含 5 个声明 Skip。其余 Skip 主要来自
未配置的 gRPC Transport，以及互斥的 Capability/Error 前置条件。Skip 不算
Pass，StateKnot 不声明 gRPC 支持。

TCK 还会按完整 Requirement/Transport Inventory 输出 `78.8%` Aggregate。
其分母包含未配置的 gRPC 与不适用分支，因此 StateKnot 同时公开 Aggregate
原始输出和精确 Pytest 数量，不把它描述成 100% 认证。

仓库校验器会拒绝数量漂移、任何 Failure/Error/Xfail、重复 Test Identity，
以及关键用例从执行变成 Skip。被强制执行的代表性用例覆盖 Agent Card Cache、
Unknown-field Ignore、REST/JSON-RPC Streaming、Multi-subscriber Ordering、带
Authentication 的 Push Delivery、Authenticated Extended Card、HTTP 415 Mapping
与 JSON-RPC SSE Envelope。

## 已审计的 TCK Compatibility Patch

固定 TCK 需要两个最小 Harness 修正：

1. `CORE-SEND-003` 的 Behavior 文本引用 `ContentTypeNotSupportedError`，但遗漏
   已定义的 `expected_error` Metadata。补丁只增加该 Metadata。上游跟踪见
   [issue #202](https://github.com/a2aproject/a2a-tck/issues/202)。
2. TCK JSON-RPC Client 对 Task History、List 与 Push-config Operation 输出
   Python snake_case Parameter，而 A2A 1.0 Schema 的 Wire Field 是
   `historyLength`、`contextId`、`taskId` 等 camelCase 名称。补丁只修正 Request
   Serialization。

补丁不会修改 Server Assertion、降低 Requirement、增加 Expected Failure 或删除
Test。只有在 Source Archive Checksum 通过后才使用 `patch --forward` 应用；
上游归档或 Hunk 发生变化都会在构建 Server 前失败。

## 复现

```console
bash conformance/a2a-server/run-1.0.sh
```

脚本验证 Supply-chain Input，构建真实 Rust Fixture，等待 Agent Card Ready，
运行两个声明的 Binding，把 HTML、JSON、XML 与 Server Log 全部复制到被 Git
忽略的 Timestamped Directory，最后执行只依赖 Python 标准库的独立结果校验器。
CI 执行同一脚本，并且在失败时也上传证据。

Runner 合约见 [`conformance/a2a-server/README.md`](../conformance/a2a-server/README.md)，
生产 Application 义务见 [A2A 1.0 Server Profile](a2a-server.zh-CN.md)。

## 声明边界

通过此门禁只证明已实现的 Server Wire/Application Boundary。生产验证仍然需要
耐久 Application `A2aTaskService`、Cross-replica Policy/Admission、
Transactional Push Outbox、Security/Failure Test、Stable API Review、Release
Artifact 与 Operations Evidence。A2A Client 和 gRPC Binding 仍是独立的未来门禁。
