<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot

[English](README.md) | **简体中文**

[![CI](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/ci.yml)
[![Supply chain](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml/badge.svg)](https://github.com/StateKnot/StateKnot/actions/workflows/supply-chain.yml)
[![crates.io](https://img.shields.io/crates/v/stateknot.svg)](https://crates.io/crates/stateknot)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**面向 Rust 的持久化 Agent 编排框架。**

StateKnot 结合强类型 Agent/Graph 契约、基于 PostgreSQL 的故障恢复以及
MCP/A2A 适配器。它是 Rust 原生运行时，不是 Python 框架的逐行移植。
阅读[中文文档](https://stknot.com/zh/docs/)或
[English documentation](https://stknot.com/docs/)。

> [!IMPORTANT]
> StateKnot 仍处于 **pre-alpha**。公共 `0.1.0-alpha.1` crate 仅供评估，
> 不代表稳定 API 或生产支持。请精确固定预发布版本，暂勿用于生产环境。

## 安装与体验

项目要求 Rust 1.88 和 Edition 2024。所有 StateKnot crate 必须固定到同一
精确预发布版本：

```toml
[dependencies]
stateknot = "=0.1.0-alpha.1"
```

在本仓库的检出目录中，本地无凭据示例可验证强类型 Agent 契约：

```sh
cargo run -p stateknot-core --example first_agent --locked
```

该示例**不会**执行持久化 Run，也不会调用模型。真正的无 HTTP 运行请看
[进程内 Agent 指南](docs/in-process-agent.zh-CN.md)：需要经过验证的
PostgreSQL 部署、显式授权、Worker 和维护角色。网络服务请看
[Agent Host](docs/agent-host.zh-CN.md) 与
[认证 HTTP 接入](https://stknot.com/zh/docs/agent-http/)。示例不会偷偷使用全放行策略或内存持久化替代品。

## 已实现能力与未完成门槛

当前预览包含已验证的核心契约、可恢复 Graph/Agent 运行时、PostgreSQL
事件日志与栅栏恢复、OpenAI Responses 和 Anthropic Messages 适配器、
有界 MCP Client/Server 与静态 MCP Skills 配置，以及 A2A 1.0
Client/Server 配置。准确实现范围和限制见[状态页](https://stknot.com/zh/docs/status/)。

**生产完整性尚未达成。** 通用数据保留与垃圾回收、完整身份与策略验收、
持久化 A2A Task/Push 服务、可观测性、OCI 角色交付、多角色故障切换、
参考负载、24 小时浸泡、升级验证和发布来源证明都是独立门槛。
单元测试或协议一致性通过不能代替它们。以
[生产收尾顺序](docs/roadmap.md#production-completion-order)、
[v1 范围](docs/v1-scope.md) 和[三个资格验证场景](docs/scenarios/README.md)为准。

## 文档与开发

- [文档索引](docs/README.md)与[中文官网](https://stknot.com/zh/docs/)
- [版本与发布策略](docs/versioning-and-releases.zh-CN.md)
- [Graph 与 Agent 示例](docs/core-contract-examples.zh-CN.md)
- [MCP 一致性](docs/mcp-conformance.zh-CN.md)与
  [A2A 一致性](docs/a2a-conformance.zh-CN.md)
- [贡献指南](CONTRIBUTING.md)、[治理规则](GOVERNANCE.md)和
  [安全问题报告](SECURITY.md)

仓库固定使用 Rust 1.88.0。本地主要检查为：

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
```

数据库、协议与发布验收还需要相应文档指定的外部服务和测试工具；
被跳过的集成测试不能算通过。官网有独立的[验证指南](website/README.md)。

## 许可证

StateKnot 使用 [Apache License 2.0](LICENSE) 开源；署名信息见
[NOTICE](NOTICE)。
