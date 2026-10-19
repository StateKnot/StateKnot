<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 可信 Root Graph 处置

本文说明 [RFC-0022](rfcs/0022-namespaced-graph-frames.md) 的实验实现契约。
实际验收范围见[当前证据报告](qualification/experimental-trusted-root-dispositions-2026-10-12.zh-CN.md)。
RFC 与 R1 仍未关闭。该报告记录历史 schema35 增量；当前 schema37 部署还须采用
[原始绝对 Wait profile](elapsed-graph-waits.zh-CN.md)和
[选中 Store Join 取消 profile](qualification/experimental-scoped-join-owner-stop-2026-10-18.zh-CN.md)，
分别保留其证据与剩余门禁。

## 创建 Root Run

实现支持两种经过认证的创建来源。Agent V1 保留真实的 Agent admission、
request、budget 与 deadline。普通可信 Graph V2 来自
`PostgresStore::admit_run`、真实的 Pending 到 Active Start，以及原始 INITIAL
Root checkpoint。Start 与 checkpoint 可以使用已有的同一复合提交，也可以使用
已有的两个独立提交。共享状态 Graph 的静态展开同样使用普通 Root 路径。

Store 从完整归属历史推导正向创建事实，绑定原始 Run provenance、admission
时间、首个事件、精确 Start revision 与时间、INITIAL checkpoint 及其归属事件。
缺少 Agent 行不能证明普通创建；Agent、submission、deadline、frame 或 child
ownership 证据不能替换这个来源。

开始新的 Root 工作前，`ClaimedRunRecovery::validate_graph_creating_source`
在只读、repeatable-read 事务中校验原恢复会话观测的精确 Root checkpoint 与
journal。它认证 canonical Run、完整归属历史及创建来源，最后检查原 fence 与
数据库时钟；仅返回 unit，不产生可移交授权或业务记录。Driver 在 cancellation
handoff 之后、节点 Start 与 frame Entry 之前执行该检查。后续每次变更与派发
仍保留原有实时校验。

经过认证的旧历史若缺少可证明的原始创建输入，会在派发前返回
`GraphReplayHistoricalDispositionUnavailable`。这是运行兼容性错误，不隔离
Run。真实归属记录损坏仍拒绝访问，并保留原有受 fence 约束的隔离行为。

## Continue、Wait 与 Terminal

V2 完整 barrier 绑定创建来源、base checkpoint、有序结果、原始完整 GraphPlan、
journal event、后继 checkpoint 与 lifecycle projection。Continue 保留 quantum、
yield 和后续 fresh claim 恢复；未来 Wait 保留原注册、部分解除、resume 和
abandonment 契约。到期后的绝对 Timer 恢复属于独立的实验性
[schema36 profile](elapsed-graph-waits.zh-CN.md)，历史 schema35 证据不授予该增量资格。

普通 Terminal 保留完整原始 `AgentResult`，以及从真实 descriptor、完整 request
和已解析 budget 得到的 `AgentTerminalValidationInputs`。
`AgentResult::validate_for_terminal_inputs` 与 `AgentResult::validate_for`
执行同一个判定逻辑，错误顺序相同。已定价及未定价费用、有限预算维度、schema
身份、artifacts 与完成时间仍须满足原有要求。这个快照不是 Agent admission，
也不是可执行 descriptor。

新提交在 SQL 锁之外调用固定版本的 schema/reducer 并比较完整 plan，原子写入
所有组成记录，执行 deferred guards，最后再次检查数据库时钟与 fence。
ACK 恢复先认证已提交的完整记录及原始校验输入，再决定是否需要新回调。
精确重试返回原提交结果，不重复 provider 工作或 evidence 回调。

input 与 state 的原有 2 MiB 上限保持不变。最大组件原生 fixture 同时提交
比原有 2 MiB output 上限小 16 KiB 的 Terminal output，并通过实际持久模型响应、
token usage、已定价费用和 invocation binding 完成账本。完整 state 仍在独立
锚定的 checkpoint 中；compact bundle 保留完整 request 与 terminal output。
V2 完整 bundle 读取上限为 14,241,792 字节，原 V1 仍为 9,474,048 字节。
这些是完整记录读取边界，不扩大各组件原有上限。

## 历史 schema 34→35 升级

当前未发布 binary 要求完整迁移至37并匹配当前54表 role profile，遵循
[当前部署顺序](elapsed-graph-waits.zh-CN.md#迁移与部署)。
以下步骤仅说明原 schema35 增量。

1. 排空旧 worker，并关闭 schema-34 runtime pool。
2. 由 migration owner 正常应用 SQLx migration 35。既有 migration checksum
   和 V1 事实不变；migration 35 不推测或回填可信创建来源。
3. 安装匹配的 trusted runtime/owner/retention role profile，核验精确 schema-35
   catalog 与 ACL。
4. 启动匹配的读写二进制，然后启用普通 V2 写入。

已连接的 V1-only reader 无法安全解释 V2 历史；本契约不支持混合版本滚动升级。
schema 不匹配的新连接返回运行错误。降级时必须避免 V1-only reader 接触 V2
历史。非空原生升级 fixture 保留既有 54 张业务及 migration 表快照、migration
1–34 checksum，以及三个原始 V1 Continue/Wait/Terminal ACK。

默认线程栈所用堆边界包裹完整的归属 Future，包括 Root 祖先认证、实时租约观测、
deadline cancellation 事务。每条 SQL、证明与 ACK 均保留，没有用 metadata
校验替换完整认证。

已通过的静态及限定原生检查只适用于记录的精确源码清单。完整 PostgreSQL 16/17
验收、最终组合的 Driver/frame/Wait 故障矩阵、真实历史版本升级及生产容量验收
仍须完成，之后才能采用为 Supported。
