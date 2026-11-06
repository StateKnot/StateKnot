<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Tool Unknown 转换的真实 COMMIT 丢失

更新日期：2026-10-27。此为实验性测试证据，不改生产 Rust、公开 API、SQL、wire 或支持级别。

writer 经公开 start/dispatch/terminal-commit 路径执行一次真实 fixture Tool 调用，将实际返回的 error/receipt 交给 controller。只有 terminal 持久化使用独立单连接 PostgreSQL proxy；controller 一次性 phase gate 将语义 Tool UPDATE 暂停至完整基线读取结束，不按第几个 COMMIT 选择目标。

BeforeCommit 不转发提交请求：回滚及真实 writer kill/reap 后仍为原完整 Executing revision1。CommitResponse 已观察服务端 COMMIT CommandComplete 与 idle ReadyForQuery、但截留两个确认：独立 owning reads 证明原 Unknown revision2、完整原 error/predecessor 与 journal/revision。提交差量只允许精确 invocation/run 列和一个 event/revision；账户、ownership 及其他租户表全部保持一致。

每个切点的新 cold 进程只接收 ownership key 与 invocation ID。它等待原30秒租约自然过期，取得更高 fence，从 Store 加载原 Prepared revision0 和原 Attempt，并使用空 provider registry：原 attempt 返回 Recovered、不业务重发；不同 attempt 被拒。Unknown reconciliation constructor 明确拒绝 Executing。claim/release 的精确租约列变化与 owning-read/refusal 完整无写窗口分别核对，旧 fence 不能释放替代租约；原 writer 和 cold observer 均真实 kill/reap。

只有已提交 Unknown 分支随后才用 controller 留存的原 receipt 重建 fixture provider。无 consent probe 被拒且不写；当前显式 consent 允许对账至 Committed revision3，并精确重放原 ACK。恢复 business sends0、probes2/denied1。Before 分支保留未解析 Executing，不重建私有 terminal handoff、不虚构自动授权处置。两分支 child 均仍 Active/unsettled；本切片不声明 child terminal/failure-close 完成、scoped-leaf Tool 路径或独立外部 provider receipt 持久化。

已实测候选为996个选定源码 `ccf157dc180a1519c490d1bddc061ef00fdbb372f72655a44168536bc948f4b5`、566编译输入 `a2b025e70cfc20bd674beafdd783dcaf3ade250facaa8adc2d9b4d7b9a63b59a`。原审批通道下六项 Rust 门禁全部通过；两次函数长度失败、沙盒回环端口拒绝和构建后磁盘护栏失败保留。复用缓存/default-feature opt1 Native 实际列出 Store276/Runtime235，非 cold build；macOS aarch64 下 PostgreSQL16/17 的精确 controller 各1passed/234filtered。lease30、READY90 与默认栈不变。marker 绑定执行断言，未另采独立 OS/SQL trace。

原始包为 `r1-tool-unknown-transition-commit-native-build-v2`（manifest `ccbf7f631022710f2de2cb502dcd3139b468b1df7b7e77f2537773e3ceb8e72d`）、`r1-tool-unknown-transition-commit-pg16-v1`（`c5617bb761fe619158bc59c792cc4a13a5bc4e5eb98f9017c81391bc50b3f994`）和 `r1-tool-unknown-transition-commit-pg17-v1`（`034f0665f23c23c7c6589c683b9961ee00de7ab752b2804db1f256fa975ae685`）。补中英说明/235库存后的新组合仍须自身验收与准确tree CI。先前 `0d0d7ab` 的[run40493667861](https://github.com/StateKnot/StateKnot/actions/runs/40493667861)已22项全绿，276/233归档保留1,018个case结果，仅属于旧tree。

另有CI leader一行修正：在打开短暂 /proc stat 文件之前先设置分组 stderr 重定向；读取成功/失败及所有 owner cleanup 条件不变。独立四项因果 probe、既有60+12 controls 和两个 Bash syntax 均通过。当前CI保留全部 jobs/profile/step mapping、原lease/readiness/pair/job预算与历史库存；新Runtime动态精确分片118/117。

cold Executing 授权处置、scoped uncertain-effect 组合、完整剩余故障矩阵、协议身份/部署、owned bounded stdio、独立消费者及 R1/R6/R7/RFC/发布资格继续开放。

[English](experimental-tool-unknown-transition-commit-loss-2026-10-27.md)
