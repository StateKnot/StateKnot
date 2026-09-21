<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 版本与发布策略

状态：这是已发布 StateKnot crate 的规范性策略。首个公共版本为
`0.1.0-alpha.1`；它是可供依赖的预览版，不代表已经达到生产就绪。

[English](versioning-and-releases.md)

## 发布包集合

StateKnot 对产品 crate 采用统一版本，并按依赖顺序发布：

| 顺序 | Crate | 边界 |
| --- | --- | --- |
| 1 | `stateknot-core` | 协议无关的公共类型与契约 |
| 2 | `stateknot-store-postgres` | PostgreSQL 持久化 Provider |
| 3 | `stateknot-runtime` | Registry、持久执行、调度与服务 API |
| 4 | `stateknot-integrations` | 模型、MCP 与 A2A 适配器 |
| 5 | `stateknot-artifact-store` | 带完整性校验的 S3 兼容 Artifact 存储 |
| 6 | `stateknot-testkit` | 与 Runtime 无关的宿主资格证据和客观评估 |
| 7 | `stateknot` | 受控 Facade、HTTP Host、Worker、Maintenance 与进程内 Agent API |

Testkit 公开发布，使 Facade 资格测试与外部宿主能够解析同一套精确版本；它的缩减
证据绝不构成生产 SLO。每个包都包含 `LICENSE`、`NOTICE` 与英文 `README.md`；
docs.rs 对同一份源码执行文档构建，并把警告视为错误。

预发布版本必须精确固定，避免依赖解析跨越未经评审的预览版：

```toml
[dependencies]
stateknot = "=0.1.0-alpha.1"
```

同一应用内的 StateKnot 产品 crate 必须使用完全相同的版本。持久化 Schema、
Registry Binding 与恢复语义以整套版本接受验证，因此不支持混用不同版本。

## 兼容性承诺

StateKnot 遵循 Cargo 对语义化版本的解释：

- `alpha.1` 等预发布标识之间允许存在破坏性 API 变更；消费者必须显式选择并
  精确固定每个预览版本；
- 发布非预览的 `0.y.z` 后，不兼容的 Rust API 变更提升 `y`，兼容新增与修复
  提升 `z`；
- `1.0.0` 后，不兼容公共 API 变更提升主版本；
- MSRV 提升会破坏受支持消费者，至少按照不兼容 Rust API 变更的级别升级版本；
- 除非文档明确标记为实验性或实现私有，导出的公共项都属于兼容边界。在安全性
  或正确性不要求立即移除时，删除前必须先弃用，并在后续不兼容版本中移除。

当前 MSRV 是 Rust `1.88.0`。Edition 2024 与一等依赖的版本要求是真实约束；
发布到 crates.io 并不表示兼容 Rust 1.83。

Rust crate 的语义化版本不会覆盖持久化数据或协议身份。带版本的 JSON Schema、
Graph/Agent Capability Identity、Provider Profile、MCP/A2A Profile 与 PostgreSQL
Migration 保留各自的显式版本，遇到不兼容漂移时失败关闭。

## 持久化数据与升级策略

- 数据库 Migration 只向前、严格有序，并在 PostgreSQL 允许时保持事务性；不得
  伪造缺失的执行证据。
- 不支持降级。如需回滚，应把升级前备份恢复到单独完成资格验证的部署。
- Release Note 必须列出每个新 Migration、持久化 Schema 变更、运维动作与兼容边界。
- 初始 Alpha 只保证 PostgreSQL 16/17 矩阵明确覆盖的升级路径；编译成功不自动
  产生更长的支持周期承诺。

## 发布门禁

版本只能从 `main` 可达、不可变的 `v<version>` Tag 发布；Tag、Workspace 统一
版本和人工维护的 Changelog 必须一致。发布提交必须通过：

1. 格式化、Clippy 零警告、全 Workspace 测试、Rustdoc 与依赖策略；
2. Linux、macOS、Windows 构建和 PostgreSQL 16/17 集成证据；
3. 冻结的 MCP/A2A Conformance 门禁与完整网站测试；
4. `scripts/verify-release.sh`，包括全部 Cargo 文件清单与源码大小上限、Workspace/Rustdoc
   重建，以及标准化无内部依赖 Bootstrap Archive 重建；下游标准化 Archive 会在同版本依赖
   依次可用后，由发布器按依赖顺序构建并验证；
5. crates.io 发布前受保护 GitHub Environment 的人工批准。

后续版本使用 crates.io Trusted Publishing：受保护 GitHub Actions Workflow 通过
OIDC 换取短期 Token，按依赖顺序发布，然后下载每个包并做 SHA-256 字节比对。
重新运行时，只有远端包与当前 Tag 构建结果完全相同才允许跳过。每个新 crate 的
首次发布是不可避免的 Bootstrap 例外：使用短期手工 Token，建立 Trusted
Publisher 记录后立即撤销。

具体操作与部分发布恢复流程见 [`RELEASING.md`](../RELEASING.md)。安全修复遵循
[`SECURITY.md`](../SECURITY.md)；安全问题可以缩短正常弃用周期，但仍必须写入
Changelog。
