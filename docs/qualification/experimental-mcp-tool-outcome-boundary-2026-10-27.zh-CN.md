<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Tool 结果共享解码

更新日期：2026-10-27。这是 RFC-0024 的私有准备片，保持现有 HTTP 行为。拥有生命周期的 stdio session 和完整进程树清理尚未实现，RFC 与 Issue 140 继续开放。

现有私有 protocol 模块独立解码 complete 与 input-required Tool 结果，不再接收 HTTP client。真实 HTTP 调用采用该解码器，并由 HTTP Adapter 将原 client、Tool 与原参数绑定到 continuation。公开类型、resume 签名、授权、header promotion、重试与 durable effect 分类保留原契约。Tool error 仍是完整 Tool 结果，与 JSON-RPC、transport error 分开；opaque state 和精确 response keys 保留，解码不自动执行宿主动作或续接。

已验收代码候选为 987 个源码文件 `43e3adf7fb881ffc9eb103c653c59f98b354d43fdfd541c741d81f8bdc4dbed1`，564 个编译输入 `ef0b090ed6b7e352127f8e8d8324363e9cf18d98183cab243bbfce2565b872ec`。本交付补文档，不改这些编译输入。本机实际证据包括：

- 原 HTTP 行为基线、4 项纯解码、5 项真实 HTTP 契约及原有 10 项协议契约通过。JSON 与分段 SSE 保留完整结果、错误和交互续接；错误 keys 不发送额外请求。
- 三项功能 mutation 分别破坏 opaque state、isError 和 response-key 校验，均触发预期运行时断言失败；它们不冒充旧实现的功能缺陷。
- mutation 全部结束且清理本任务 Integration 包缓存后，最终六项 Rust 门禁通过。首次 Clippy 因测试字面量含分解 Unicode 序列失败的记录保留；改用 Rust 转义仍保留运行时 `65cc81`，不规范化数据或添加 lint 例外。
- PostgreSQL16/17 各在全新 template0 UTF8 数据库强制执行全部 6 项 durable MCP/A2A 用例，均通过并各留下 6 条真实 Run。此前缺数据库配置的 harness 返回不代替这些实测。
- 官方 runner 固定 `0.2.0-alpha.11`、要求固定 `2026-07-28`，Client32 和 Server37 个必需场景通过，required failure 均为 0。Client7 个不计分场景仍有 17 次失败；10 个任务扩展仍有 30 次失败，未扩大支持范围。

保留首次协议运行的必需 MRTR 场景失败：失败后保存的二进制在同一场景再次出现 4 个失败检查，清洁重建二进制在源码不变时通过全部 5 个检查；随后完整冻结 Client/Server 门禁通过。两份二进制、原始报告及 A/B 对照均保留在本机交付证据。尚未证明底层 Cargo 缓存失配的精确机制；后续可变负向控制须使用独立空 target 目录，与正向验证缓存分开。

独立源码、style 与数据审查绑定候选和实际收据。旧提交的 CI 不转授本增量，准确发布提交仍须完整 CI。原生 containment、资源上限、公开 stdio 配置、平台崩溃证据和精确 pin 的外部消费者继续作为 RFC-0024 的接受条件；不关闭 stdio、发布资格或 R1。

[English](experimental-mcp-tool-outcome-boundary-2026-10-27.md)
