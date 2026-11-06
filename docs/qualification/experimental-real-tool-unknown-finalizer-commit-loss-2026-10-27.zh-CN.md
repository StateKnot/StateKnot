<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 真实 Tool Unknown 对账后的父 finalizer COMMIT 丢确认

[English](experimental-real-tool-unknown-finalizer-commit-loss-2026-10-27.md)

状态：已有准确隔离本机定向证据；本次新组合仍待验证。本增量在[原真实 Unknown 账户用例](experimental-failure-close-real-tool-unknown-2026-10-21.zh-CN.md)之后增加 test-only characterization，不新增生产接口、实现、schema、迁移或业务 backend，不接受 RFC-0022，不关闭 R1/R6/R7。

## 已执行源码与保留回执

实际执行的冻结源码为 r1-real-unknown-finalizer-fault-source-v3：977 文件，canonical SHA-256 为 ce3b2c6c43fa654c3174f1badc08cd0e9719f7d992614b48347be7d41cf1563b；564 编译输入摘要为 9b4854020bb8e3bfd54ebc54fd6e4c807ab8b8f83ae94e6e05e5e61c9bba1829。它们只标识已有隔离执行，不代表后续组合。

.local-verification/2026-10-18 下保留：

| 回执 | SHA-256 |
| --- | --- |
| r1-real-unknown-finalizer-fault-exact-local-acceptance-root-v1.json | 113988dfba8770d9d60c2348787aea25784b39c9c6fee0f390328615200b9137 |
| r1-real-unknown-finalizer-fault-independent-data-review-v1.json | fb0004a7e2607c26857a80abfe70f55b74275fdfb5ad03663a523018a09e5047 |
| r1-real-unknown-finalizer-fault-native-build-v1/manifest.json | bbe18cc5ffc1ead60d3c90767076399484774bd93520d119957fb3460a872ce0 |
| r1-real-unknown-finalizer-fault-pg16-v1/manifest.json | 276cdb4e9430f4f8dd08ac37d6bcaf419d5754bf1f3a56e187291e13273b2592 |
| r1-real-unknown-finalizer-fault-pg17-v1/manifest.json | e21b63a9f94c320e618891edf3eb5d06532b71830f24db1da54b1801d252237e |

default-feature 双角色 Native no-run 构建实际 exit0。Core、Store、Runtime 库和 Store 测试 binary 均为 fresh 缓存，仅 Runtime 测试 binary 重编；真实库存为 Store276 / Runtime231。这不是冷构建或完整 Native 执行。同冻结源码严格 workspace/all-targets/all-features Clippy exit0；此前 manual_assert 的 Clippy101 仍保留。

精确 controller child_admission::ownership::failure_close::real_unknown_finalizer_faults::real_tool_unknown_finalizer_commit_loss_survives_kill_and_cold_owning_reads 在两个 PostgreSQL major 各通过一次、230 filtered、零 failed/ignored/measured。宿主为 macOS aarch64，PostgreSQL 容器的 fsync、synchronous_commit、full_page_writes 均开启。

| PostgreSQL | libtest 秒 | capture 秒 | writer 切点 | 有序前置检查 |
| --- | ---: | ---: | ---: | ---: |
| 16 | 29.17 | 29.190913 | 2 | 36 true |
| 17 | 13.34 | 13.367272 | 2 | 36 true |

## 已同意的 seam 与实际断言

夹具先产生真实 Tool Unknown/ReconcileFirst，保存原 parent failure 和 child cancellation；未完整结算时精确拒绝父完成且全租户无写。原 invocation/schema/operation 的无 consent probe 被拒绝，随后显式授权真实对账；完整原 receipt、重放 ACK 与一次业务发送均核对。child 再通过公开 Loop 终态并结算 USD11，之后才创建 finalizer 子进程。provider receipt backend 一直留在 controller 中，没有重启。

两个切点均针对 public tick 到达的父 FailureCloseCompletion。BeforeCommit 不转发 COMMIT；CommitResponse 观察到 PostgreSQL 的 COMMIT CommandComplete 与 idle ReadyForQuery 后，将两帧响应截留，不交给 writer。两个 major 共四次 writer 真实 SIGKILL/reap。Before 必须全租户不变；After 仅允许父终态 delta 和一个父 journal event。controller 等待实际 backend 断开。

新进程只接收 tenant/run 身份，重建 Store、调用 public tick，不注册 provider。Before 恢复一项，After 返回零项。After 空报告不是 ACK；公开 owning reads 比较原完整父 journal event、run head、failure-close record 和 child receipt。完整 account 必须等于 pre-terminal account.observe_direct(原 terminal ACK, 原 frozen DIRECT)，保留 costs[]、精确 child/delegated settlement，并且只累计一次。cold worker 和 controller 均再次 tick、完整 owning reads，并断言全租户无写。marker 保存 event/account 摘要；完整 typed equality 由已执行 Rust 源码断言。

## 本次组合与资格限制

本次 payload 采用已格式化、修复 manual_assert 的 v3 原字节：failure_close.rs 仅模块注册，failure_close_completion_commit_loss.rs 仅两个 pub(super) 测试 helper，另加 characterization 模块。新模块 SHA-256 为 6eb46db851b808d7abe48c4a7bae5b8ba5ebe0036d41d0f4c621c8b5b1399e2a。生产源码不变。

当前 CI 库存守卫更新为 Store276 / Runtime231。两个 Runtime lane 仍按完整真实清单排序后交替派生：passed116/115、filtered115/116，互斥且 union 恰好一次覆盖完整库存。现用 Return/Wait exact marker 改为 1/230。default features、debug assertions、overflow checks、默认栈、lease30、READY90、pair1800、Native30min、owner protocol 和 legacy 证据策略不变。

新增 local-ce3b-runtime-inventory.stdout 是真实本机 build 的原 24,387 字节清单，SHA-256 ba9106863109842a771ff57f1bf1af7fbba476be72da70118c79f59355a14703。current-provenance.json 分别标识该 macOS source/build 与保留的 c9 Linux-CI Store 清单。原 c9 两份 list 和历史 provenance 原字节保留，旧 Runtime 仍为229。单元测试的 artifact-shaped receipt 使用明确标为 synthetic 的共同 source binding，不把任一清单冒称为新 Linux CI。

基于 b5144d44187cec87dbfecf1cf1b5309974267071 的新组合须建立自己的 source/compiled identity、六静态门禁、60 项 shard codec/unit controls、现行 artifact controls、实际 default-feature 库存、完整双 PostgreSQL Native 与准确 head 的 Linux CI。未来摘要和结果不预填，旧 CI 与旧隔离 source 的定向通过不转授新组合资格。

未解析 Unknown 跨进程丢失、provider backend durability、该 profile 的 Store Existing replay、独立 Driver mutation 中断、跨 OS kill 及更广 effect/fault matrix 仍开放。R1/R6/R7、RFC 接受、MCP stdio、Supported 和发布资格仍开放；此前所有 source-specific 证据与失败保持原范围。
