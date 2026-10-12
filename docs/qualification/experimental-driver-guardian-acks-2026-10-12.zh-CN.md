<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 Driver 租约监护确认验收

本组件绑定提交 `32c130195399a10d6a9f9b0ee81323ea1ea75df4`、tree
`2c9128e37c19e24889b3e3ad9bccce34d9dd8922`：完整 884 个源码文件，manifest
`127373c9cb1838429d0fd9c4bb93710d0bfc669d87514e8adf283b3af16a744b`。
全部提交 blob 与实际编译、测试的输入逐字节相等。后续普通 Root/schema-35、
过期等待/schema-36 和 scoped Child Join 组合须分别验收。R1 与 RFC-0022 仍开放。

[English](experimental-driver-guardian-acks-2026-10-12.md) ·
[机器可读证据](experimental-driver-guardian-acks-2026-10-12.json)

## 原失败与修复行为

外层 Driver 监护原来会在租约观察失败时丢弃 owning recovery。真实 PostgreSQL 16
已提交 quarantine、扣留 ACK 31 秒的用例，在读完原 quarantine ACK 之前返回
连接池获取失败。现在监护发出协作停止信号，并等待读完原 drive 与已开始的维护操作。
完整 owning reader 和最终 live fence 检查继续决定执行权限。

内层节点监护原来在原两秒单调租约期限丢弃已提交的续租 ACK。真实失败、确切源码、
二进制和日志均保留。应用任务仍在原期限停止；Driver 随后读完原确认，再返回
原 `LeaseExpired`。迟到的 ACK 不授予新执行权限。

节点完成、外部 shutdown 两项对照分别还原原执行方法，均真实失败：续租 ACK
被扣留时，同一 PostgreSQL 会话已收到两条后续 frontend 消息。修复后连接仍由原续租
持有，释放 ACK 前后续消息为零。独立数据库读取同时核对已提交租约，以及释放前
不变的 journal/checkpoint。不记录协议内容或凭据。第一次绿跑在准备阶段到期的失败
单独保留；夹具把原两秒 claim 移到连接/catalog 准备完成后，原受测期限与断言不变。

## 实际验收

| 边界 | PostgreSQL 16.15 | PostgreSQL 17.11 |
| --- | --- | --- |
| 外层监护失败后的 quarantine ACK | 通过 | 通过 |
| Start ACK 后更高 fence 接管，应用调用为零 | 通过 | 通过 |
| 原 fence 仍存活时保留 Start ACK 并协作停止 | 通过 | 通过 |
| 原执行期限之后读完续租 ACK | 通过 | 通过 |
| 节点完成后，提交结果前读完续租 ACK | 通过 | 通过 |
| 外部 shutdown 后，释放所有权前读完续租 ACK | 通过 | 通过 |

两版各六项矩阵使用同一实际编译二进制与完整源码，并保留首次实际编译记录。
默认线程栈、原两秒测试租约、默认 30 秒租约/连接池超时与外层 31 秒 cut 不变。
同一源码通过严格 workspace/all-targets/all-features Clippy、98 个工作区 suite /
2,036 项测试（6 项既有 ignored）、45 项独立文档测试、严格 Rustdoc、格式检查及
固定 `nightly-2026-10-07` 全工作区检查。未配置数据库环境的工作区测试不构成
数据库原生验收。

一名独立评审者按 Standards/Spec 两轴检查了未变的生产监护方法；后加的测试协议
观察器不继承该先前评审。完整源码、实际二进制、诊断、日志和收据保存在构建缓存
之外的 `/private/tmp/stateknot-driver-complete-ack-qualification-v1`。

完整 175 项 Runtime 数据库测试随后使用该保存二进制结束：PostgreSQL 16 为 152 项
通过、23 项失败；PostgreSQL 17 在更高 fence 嵌套用例发生默认栈溢出并终止，没有
完整 libtest summary。原日志、崩溃报告与二进制均保留，完整 Native 不合格。
最终四套完整原生组合、深层/多个 namespace Child Join 与 AgentLoop 默认栈、
历史读取成本、Linux、完整进程/COMMIT 故障矩阵、真实已发布 N−1/N−2 升级与
R6/R7 生产门禁仍须完成。本组件不提高租约/资源上限、不替换为 metadata 认证，
不接受 RFC 或宣称已支持生产发行。

后续 scoped 注册 COMMIT 丢确认矩阵发现独立的结果优先级缺口：原完整注册恢复已完成，
但监护 `NoActiveLease` 覆盖了已确认的 `ChildJoin` 结果。该确切源码的失败保留于 JSON，
继续修复。完整 Runtime 另暴露测试阶段区分缺口：按全局续租次数切断可能在实际 Node
调用前命中准备续租。下一测试观察真实 executor 调用后才切断，原两秒期限不变。
这些结果不抹去六项实际子范围通过，也不授予后续修复资格。

后续窄范围释放结果修复与新的七项监护矩阵另见
[独立组件报告](experimental-driver-release-acks-2026-10-12.zh-CN.md)，不覆盖本报告源码。
