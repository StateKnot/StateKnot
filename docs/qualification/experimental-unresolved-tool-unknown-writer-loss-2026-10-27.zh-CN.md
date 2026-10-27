<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 未解析 Tool Unknown 跨 writer 进程丢失

更新日期：2026-10-27。本测试切片补充实验性 RFC-0022 的故障证据，不改生产 Rust、公开 API、SQL 或支持级别。

被终止的 writer 自己执行真实 durable Tool attempt，保存 Unknown/ReconcileFirst、父 failure-close 与 child cancellation 事实，再把实际原 receipt 交给测试 controller。该切点没有对账 consent、child terminal usage 或 settlement；controller 真实 kill/reap 原 writer。测试 controller 留存原 receipt 不等于外部 provider 的独立持久化。

新的 cold 进程只接收 key 与 invocation ID，不接收 provider 或 receipt。它等待原 30 秒租约自然过期，取得更高 fence，再调用真实 DurableGraphDriver；观测为 CancellationRequested handoff，普通应用调用与 durable events 均为 0。随后显式 release，再被 kill/reap。claim、续租和 release 是允许的真实写入，只准精确 child 的租约/排程列变化；完整账户、原 Unknown history、ACK 和其他租户事实不被过滤。owning reads 和拒绝检查分别在允许操作之后建立完整无写窗口。

完成 cold proof 后才用原 receipt 重建测试 provider。无 consent 的 probe 被拒且不写；对原 invocation/schema/operation 显式授权后才允许 reconciliation。断言分别观察原 writer business sends1、恢复 provider sends0、probes2 与 denied1；原结果及 ACK 重放不变。child 完整独立 settlement、USD 委托成本 11 micro-units、完整父子账户、terminal delta 和完整 ACK 均核对，再做最终无写重读。

已实测代码候选为 992source `f2089d5a48d9288cc1241b0674235d17d13a024751b41fb0e9f0249092c6d939`、565编译输入 `7964eb40eee79683f35c5ec6cb9677bf0751f597e031065261ca92842617df30`，六项 Rust 门禁通过。首次 strict-Clippy similar_names 失败及仅六处标识符重命名的后继记录保留。真实 default-feature Native 构建列出 Store276/Runtime233；macOS aarch64 下 PostgreSQL16/17 的精确 controller 各1pass/232filtered，用时34.29/33.98秒。lease30、READY90 和默认栈不变。

原始证据包为 `r1-unresolved-unknown-writer-loss-native-build-v1`（manifest `235f484f7a0bc4ac896fd1137aab4565a643a83be7bee2bf51259dcde4df44fa`）、`r1-unresolved-unknown-writer-loss-pg16-v1`（`d69f28dbb5e4c8793aa4016a6a11266671e4b7b6956751a1f06e5c88c5175489`）和 `r1-unresolved-unknown-writer-loss-pg17-v1`（`b6d8a0800d26812737332f50ae179a9b3e609f21776482b61c4af5fe11604df6`）。marker 保存完整账户/ACK 与摘要绑定；租约时序、SQL snapshot 和 probe 次数由实际执行的断言保证，未另存全部原始 snapshot。

本交付按真实233清单及明确本机 provenance 同步当前 CI 库存/Return/Wait guards，历史229与231夹具保留。Runtime 动态精确分片117/116，Store276、全部 step mappings、profile、owner protocol、pair1800秒与job30分钟保持不变。补文档/CI的新组合仍须自身验收与准确head CI，旧代码候选不自动转授资格。

Unknown 事务 COMMIT 歧义、独立 provider 持久化、跨 OS kill 与完整剩余故障矩阵继续开放；不关闭 RFC-0022、R1、stdio 或发布资格。

[English](experimental-unresolved-tool-unknown-writer-loss-2026-10-27.md)
