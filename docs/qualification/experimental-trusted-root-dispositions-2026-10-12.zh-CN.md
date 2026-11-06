<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 可信 Root 处置实验验收证据 — 2026-10-12

普通 Root V2/schema-35 实现及完整归属 Future 的堆边界已通过静态检查和下述限定
原生检查。**完整原生验收仍未通过**。RFC-0022 仍为 Draft，R1 仍未关闭。
参见[采用契约](../trusted-root-dispositions.zh-CN.md)与
[机器可读证据](experimental-trusted-root-dispositions-2026-10-12.json)。

## 精确源码与执行环境

生产提交链为带 DCO 的 `3c15c595f2395e48e21c943d08784cfdfb11a7ec`，随后是
`630e4c68fbe167234c9957aa5580ca230e510e09`，tree 为
`dac011e6dec01984b68db5a083d10456cc1397ec`。875 个跟踪文件的哈希与冻结源码
`/private/tmp/stateknot-ordinary35-private-future-source-v7` 完全一致；manifest
SHA256 为 `6f7dfde47feeb762a0b2dc8c18e6e749eb4eaedd70b66734e34c46c77bae733b`。

最终源码首次编译前 stable cache 为空，Cargo 日志记录 Core、Store、Runtime
从该确切源码路径编译。限定原生 Runtime 二进制 SHA256 为
`83499cae7beeed61c9ee657acbad5a0ab001a79e165fc4d3d7d0e3ac2a5b6f30`。
receipt 绑定执行前后源码及二进制。PostgreSQL 16.15/17.11 使用固定的本任务镜像、
容器身份与标签；各限定组独立使用 template0 空数据库，起始没有用户表或 migration
metadata。普通验证仅关闭增量编译和调试符号，原默认线程栈、lease、deadline
与断言均保留。

本报告与采用文档属于后续独立文档提交。代码资格只绑定以上源码，不向后续最终
组合 tree 转移。

## 该源码已通过的范围

| 检查 | 实际结果 |
|---|---|
| 固定 nightly `2026-10-07` 的全工作区/all-targets/all-features check | 通过 |
| Format 与完整工作区严格 Clippy | 通过；没有新增 warning allowance |
| 完整工作区/all-targets/all-features/locked 测试 | 2,014 通过，6 忽略 |
| 单独运行工作区 doctest | 45 通过 |
| 严格工作区 Rustdoc | 通过 |
| PostgreSQL 16/17 普通 Root 原生组 | 各 9 通过 |
| PostgreSQL 16/17 schema 35 完整原 trusted role fixture | 各 1 个实质 fixture 通过 |

工作区测试未设置数据库环境，PostgreSQL 入口会提前返回，不能作为原生资格。
首次 sandbox loopback 权限失败与随后同源码获授权的 loopback 成功均保留。

九个普通 Root 案例包含独立 Start/INITIAL 的正向历史与组成记录替换回滚；旧创建
输入不可证明时零 executor 调用；精确 recovery-session/journal/fence 绑定且零
写入；完整 Terminal ACK 恢复；真实已提交、已定价持久模型调用下的最大
input/state/output；原 known/unpriced cost 错误顺序；新来源替换回滚；最终数据库
时钟到期回滚。完整 role fixture 保留原 ACL drift、framework/provider/Join、
scoped 操作、非空升级及 24 次竞争检查；不能据此宣称已完成 10,000 次竞争或容量
验收。

## 保留的原生失败

先前冻结的 `3c15c595` 源码在两个 PostgreSQL 版本上运行了**全部** Store 与
Runtime 原生测试，806 个实际测试中 803 通过、3 失败。这些 receipt 与最终堆边界
源码的限定资格保持独立。

| 源码 / 套件 | 原始结果及剩余失败 |
|---|---|
| `3c15c595`，PostgreSQL 16 Store | 251 通过 |
| `3c15c595`，PostgreSQL 17 Store | 249 通过、2 失败：generic Root 8/16/32 构造在 application replay 前超过原 lease；绝对 scoped Wait 在注册前已到期 |
| `3c15c595`，PostgreSQL 16 Runtime | 151 通过、1 失败：orphan 并行 sibling 超过原 Driver 五秒 timeout |
| `3c15c595`，PostgreSQL 17 Runtime | 152 通过 |
| 最终源码 PostgreSQL 16 限定 deadline 组 | 11 个被选择测试通过 |
| 最终源码 PostgreSQL 17 限定 deadline 组 | 10 通过、1 失败：原两秒 deadline 在 Waiting 提交前到期 |

两个 deadline 组合计包含四个由输入环境控制的 process helper 入口；它们记录为
被选择测试，不作为独立 process-loss fixture。最终限定组共选择 42 个测试，
41 通过、1 失败。

失败的 PostgreSQL 17 数据库显示 admission 到 Start 用时 1.034662 秒，Start
到 Success 用时 0.831139 秒，Success 仅比原 deadline 早 22.706 毫秒。Run 仍为
Active/J3，没有 Waiting 提交或隔离，cleanup 已释放 lease。这是实际 deadline
失败，不能解释为原 30 秒 lease 用尽。

2026-10-12 的一次有依据诊断使用**同一不可变二进制**、新的 template0 数据库，
保持原 deadline、lease 和默认栈，实际 1/1 通过并完成真实 Waiting、deadline
cancellation、abandonment 与 closure。Start/Success/Waiting 间隔分别为
0.243568/0.130497/0.215859 秒，记录了本次真实容器启动时间和重新核验的 loopback
端口。这个诊断不替代原失败组，也不证明生产阶段成本目标已经满足。

完整最终组合原生检查、scoped ChildJoin 与 Elapsed36 组合、原阶段成本/容量目标、
真实已发布 N-1/N-2 升级仍须完成。没有为了取得这些结果放宽限额、完整归属认证、
corruption 规则或预期断言。
