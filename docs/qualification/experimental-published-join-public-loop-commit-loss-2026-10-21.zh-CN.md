<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 已发布 Join 取消：公开 Loop 的 COMMIT 丢确认

本记录分别绑定已发布基线的完整 CI 与新 test-only 候选的本机定向实证。[English](experimental-published-join-public-loop-commit-loss-2026-10-21.md) · [机器记录](experimental-published-join-public-loop-commit-loss-2026-10-21.json)。R1、RFC-0022 接受和发布资格继续开放。

## 已发布基线

Head `53b0d83e226954c67004de5ff28dccd6208101e4`、tree `8d8793dee9cea47daf12cf875cab6f51d1c6074c` 的完整 [CI 39444455774](https://github.com/StateKnot/StateKnot/actions/runs/39444455774) attempt1 已核验；实际 checkout 为 `312e54f136b0ec625f25ede1eb4672c435a59546`。

| 绑定范围 | 实际结果 |
| --- | --- |
| 选中源码 / 编译输入 | 969 / 563；source `fbc2dc9c…`、compiled `3c6dd428…` |
| CI | 22/22 job 成功、20 份 checkout 日志、56 个 artifact、120 个必要 step 状态 |
| 严格分析 | 15 个组件及 111 个负控通过，无待定正控目标 |
| 完整 Native | PostgreSQL16、17 各 Store275 + Runtime227，共 1,004 项通过 |

这些结果只属于上述已发布源码；下述新公开 Loop 用例不在其 Runtime227 库存内。旧失败、中间证据及其原有限制保持不变。

## 本机定向候选

隔离的 `published-join-v3-public-loop-minimal-source-v2` 为 969 个选中源码文件、563 个编译输入：

- source：`1424fc42fd41f3bdb9242b6b057572e4d1de9600904119b6a2c091c89cd8f41f`。
- compiled：`54c23f837b63b150aae70fa306cd1c2d6860b4c05ebc9e96b55af7d7eb0276d5`。

相对已发布基线仅两个测试文件 `scoped_child_join_fresh_red.rs`、`scoped_child_join_parent_terminal.rs` 变化，生产 Runtime、公开 API 与 SQL 不变。实际构建库存是 Store275、Runtime228；本轮定向捕获没有执行完整 Store 或完整 Runtime228 suite。构建合法复用缓存，不称冷构建。

| 本机执行 | 结果 | 实际墙钟 |
| --- | --- | --- |
| PostgreSQL16.15 | 1 passed、0 failed/ignored、227 filtered；两个 COMMIT 切点 | 82.832 秒 |
| PostgreSQL17.11 | 1 passed、0 failed/ignored、227 filtered；两个 COMMIT 切点 | 82.479 秒 |

Harness 为 macOS aarch64，数据库运行于 PostgreSQL 容器。Native 使用默认 features/线程栈、test opt1、debug info0，保留调试断言和溢出检查；原 30 秒租约、90 秒进程 readiness、1,800 秒 suite watchdog 与 Native CI job30分钟均未扩大。另一个 opt0 workspace/all-targets/all-features 严格 Clippy 预检 exit0，不等于组合六门禁完成。本轮不授 Linux 宿主或性能/容量资格。

## 已验证边界

Writer 通过 `DurableAgentLoop::run` 进入；Driver 先返回取消 handoff，随后 Loop lifecycle 发起 v3 closure 事务。因此 `public_loop_lifecycle_mutation_interrupted=true`、`public_driver_handoff_observed=true`，而 `standalone_driver_mutation_interrupted=false`。

每个 PostgreSQL 版本均实际执行以下两个切点，并真实 SIGKILL/reap 自己拥有的 writer、等待指定数据库后端退休：

1. **COMMIT 未转发**：以真实同 fence guardian 续租 ACK 后的完整 tenant 快照为基线，保留所有租约字段；切点与 writer/backend 退休后全快照相同。此时无 closure，ID-only finalizer 先返回 NotApplicable。冷进程仅接收 tenant/run，从真实父 admission、完整子账户、原 child intent 和持久 graph pins 重建 registry，重读包含真实取消事件规范字节的完整 DIRECT；等待原租约在真实 DB 时钟自然到期，再 claim F3 并调用公开 Loop。
2. **COMMIT 响应被扣留**：原完整 v3 closure 已持久化。冷进程在构造应用执行注册表（ExecutableGraphRegistry）或 application provider 前使用 ID-only finalizer，保留原 closure 字节和历史 ACK；终态确认只提交一次，随后返回同一事件的幂等回执且不再写 tenant。

两条路径都保留原普通 owner 的 Executing 状态以及完整 request/registration/binding/publication；不写普通 owner completion/result，不消费 Join、不执行 resume callback。完整已定价 DIRECT 加真实已结算 child 的两次 model turn、一次 tool call 只累计一次，新 executable 的应用计数全部为零。这是该限定 profile 的 owned writer/backend 收束证据，不是一般后代进程树 owner 或 R1 完整故障矩阵。

marker 字段 `registry_or_provider_constructed=false` 中的 registry 指应用执行注册表 ExecutableGraphRegistry，provider 指 application provider；冷 finalizer 仍会通过 `cancellation_schemas()` 构造自己的 JsonSchemaRegistry。

## 证据与后续

机器记录固定两份各自 source-specific 的 Root 索引、实际构建、两份各95文件 capture manifest、原始 stream 摘要、原 closure/terminal 摘要与四个 cut。定向接受索引为 `published-join-v3-public-loop-directed-acceptance-root-v2.json`，SHA-256 `5f9a16b827482dc8c7ea9c81278b3e66f9adb32369e8a62c9992d204c6d4d006`；原始捕获零脱敏。

首个候选构建因两处非法 `json!([0; 6])` 宏表达式 exit101，没有运行用例，不算功能 RED。其冻结源码和失败构建原样保留；authoring-v2 仅将这两个 JSON 期望数组写成六个显式零。

新组合候选仍须完成自己的六门禁、完整 Native、发布和 CI 身份。加入本文不会扩大本机 minimal 源码身份，也不转授旧 CI 结果。独立 Driver mutation 中断、更大 R1/R6/R7 门槛、MCP stdio、Supported 与发布资格仍开放/未裁定。
