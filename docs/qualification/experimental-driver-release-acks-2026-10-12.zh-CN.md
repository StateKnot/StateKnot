<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 Driver 已确认释放结果验收

DCO 提交 `ff081612853dcdb3094297542d3d0739e8383496`、tree
`0ee75edc263b54ffd7793f44248aa3b2d8c15271` 与实际编译、测试的完整 896 个源码
文件逐字节相等，manifest SHA256 为
`4fa93cf9c8d7d8e7c3c910f82fb9a1110bff5f55a9c94d15eb63311c7662a1bb`。
验收仅覆盖明确组件范围；RFC-0022 和 R1 仍开放。

[English](experimental-driver-release-acks-2026-10-12.md) ·
[机器可读证据](experimental-driver-release-acks-2026-10-12.json)

原监护会读完 owning 写入，却用 `NoActiveLease` 或连接池错误覆盖已成功释放所有权的
结果。真实 scoped Join 注册丢确认首先暴露该问题。独立公开 Driver 回归确认租约释放、
完整 lifecycle/journal/checkpoint 不变，并将实际 COMMIT 响应扣留 31 秒，随后复现错误的
连接池失败。两份真实失败均完整保留。

Driver 现在在读完完整 Store ACK 后保留 `ChildJoin`、`FrameWaiting`、已释放的
`Yielded` 和已调度的 `Deferred`。`Cancelled` 仅在监护清理之前外部 shutdown
已经请求，且原释放 ACK 完成时保留。监护内部取消不能掩盖监护失败；仍需 live
权限的交接与 `Deferred::Due` 保持原失败行为。完整 owning reader、原 fence、时钟
和写入不变，本修改不增加执行权限。

PostgreSQL 16.15、17.11 各七项真实监护回归全部通过：quarantine ACK、Start ACK
后更高 fence 接管、原 live-fence 负例、原执行期限后的续租 ACK、节点完成、外部
shutdown，以及已确认所有权释放。两版使用同一实际二进制。原两秒执行租约、正常
30 秒租约/连接池期限、31 秒 ACK cut 与默认栈不变。续租 cut 观察真实 executor
调用，Start 与 quarantine 在连接准备完毕后开始受测两秒租约；quarantine 在正常
准备之后通过真实公开接口取得更高 fence。首次故障注入前到期的准备失败单独保留。

同一精确源码通过格式检查、严格全工作区/all-targets/all-features Clippy、98 个工作区
suite / 2,047 项测试（6 项既有 ignored）、45 项独立文档测试、严格 Rustdoc 和固定
`nightly-2026-10-07` 全工作区检查。未配置数据库的工作区条目不构成 Native 证据。
两名独立评审者分别检查固定 patch 的 Standards/Spec 两轴及随后仅夹具变化，均无
可操作发现。完整源码、二进制、原失败、日志、评审和收据保存在构建缓存之外。

该精确源码的十三项普通 Root Runtime 回归在两版数据库上均为十一项通过、两项
失败。两个原五秒并行 Driver 用例在三个分支完成之后超时，仍待修复。此前独立
schema34 ACK 源码的完整 Runtime 在 PostgreSQL 16 为 152 项通过、23 项失败；
PostgreSQL 17 在更高 fence 的嵌套用例发生默认栈溢出并终止。原失败没有删除，
也不归因于新源码；先前六项监护通过仍仅属于当时精确源码。

最终 schema35/schema36/scoped Child Join 组合、四套完整 Native、深层/串行/
AgentLoop 默认栈恢复、历史读取成本、Linux、进程/COMMIT 故障矩阵、真实已发布
N−1/N−2 兼容及 R6/R7 生产验收须另行完成。本报告不接受 RFC，不宣布生产发行
或 Supported。
