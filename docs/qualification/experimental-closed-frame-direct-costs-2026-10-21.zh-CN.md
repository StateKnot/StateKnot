<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# child 引入新币种时保留 closed-frame 原 DIRECT

[English](experimental-closed-frame-direct-costs-2026-10-21.md)。本记录仅授予本机定向范围，不是完整 Native/CI；R1、R6/R7、RFC-0022、MCP stdio、Supported及发布继续开放。

## 三条私有生产路径

真实已结算 child 有 USD11微单位，而原完整验证的取消 closure 冻结 DIRECT costs[]。正确 parent total 在原终态减法中保留显式 USD0，无法与原空 DIRECT 精确相等，返回 IncompleteChildAccounting。双版均在 setup 成功后复现，拒绝后全租户状态不变。

修复改变 Store 的 child_runs.rs、failure_closes.rs、graph_frames.rs 三条私有生产路径：完整认证原 failure-close 或 owning frame-closure，用原冻结 DIRECT + 实际 settled delegated usage 正向校验完整 total，并保留原 DIRECT 表示；两份冻结来源同时存在时必须完整相等。两来源都无时保留原减法，原算术错误优先级、settled-account、SQL/锁序和历史 ACK 优先不变。Core明确保留零币种项的语义、公开API、SQL、迁移和 wire schema均不改。这是生产修复，不是仅测试修改。

## 绑定实际源码的定向结果

| 绑定 | 双版 RED | minimal GREEN |
| --- | --- | --- |
| 974源文件 | `e3624bc4724f222adb897eb6a4b6799e2c0da9fbb9d6def51dc902df427dd2aa` | `16863bd82259968993a4686a810d7107dc06f2e4c328ab4fff50afd464da98b7` |
| 563编译输入 | `1094d1d409fa0f7226813da9f88c993a6b8d71782910cf8a2e5cc19ea7dff57f` | `48eccc16d58f076e2395094010abb6a30ea1169c607c3b8426f08f47eb33e08e` |
| PostgreSQL16.15/17.11各exact | exit101，0passed/1failed/275filtered | exit0，1passed/0failed/275filtered |

RED/GREEN整个 Store 测试文件完全相同，SHA256 `6becba10c74881a8a358f035d32c0599f3938e98e5fb2d75788668f498ca4276`。RED Root83a8e2b5…及独立证据审20bdff45…保留原 setup/失败/清理事实。GREEN Rootc4713f09…绑定prep936a4308…/buildab840711…，实际库存 Store276/Runtime229。原 owning closure、owner、完整 child settlement、完整账户累计一次、终态 DIRECT head及原 ACK/replay均已走过，清理先于最终 verdict；不能把两次 exact 通过称为完整276/229通过。

同一 minimal GREEN源码/构建的 D3真实Tool Unknown回归双版各1passed/228filtered，四个A/B profile共116true checks（Rootb4cb757e…）。另五个既有 guard case各在双版新库通过，共十次 exact（Rootbf13af38…）。合计七个选中 case×双版=14次定向，不等于完整故障矩阵或全部 Native库存。

宿主为 macOS aarch64配 PostgreSQL容器，保留 default features/stack、Native testopt1/debug0/assertion+overflow；原lease30秒、READY90秒、full-pair1800秒、Native job30分钟不变。不据此声称 Linux宿主、性能或容量资格。

## 原负例与后续门禁

旧 D3组合 source6baea426…/compiled872a34f7…的四套完整测试负例仍保留（Rootb13205d3…）：都到原1800秒 watchdog后 exit-15，原始流完整捕获但无完整 harness summary；输出中的 FAILED前缀分别为PG16 Store12/Runtime5、PG17 Store3/Runtime6。这些属于旧源码，不是当前16863bd8…的结果；旧定向通过和本次D4通过都不能抹去失败，也不据此认定唯一环境或源码根因。见[D3追加记录](experimental-failure-close-real-tool-unknown-2026-10-21.zh-CN.md#2026-10-21-完整-native-负例追加)。

minimal GREEN同源严格 Clippy实际101、35.227秒，仅三个新增测试 helper超过100行：settle_priced_child106、terminal_and_replay125、characterize126，logcd81b678…。该次未报告生产编译诊断，但不是静态通过。另封 USD12错误总量 guard稿只有源码审查，未运行/采用；其69062字节标签多计一个分隔LF，真正保留的原文件是69061字节，旧稿和元数据差异原样保留。

后继 wrong-total与连贯 helper提取组合须独立测量新 source/compiled、fmt和完整六门禁、双版完整 Native及新发布/CI。最终组合源码/静态/Native/CI与任何 Draft发布 head均待实际，本文不预填。普通 Root barrier及更广 Unknown/effect/故障组合仍独立开放。
