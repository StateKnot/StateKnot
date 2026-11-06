<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 Driver 完整私有阶段

[English](experimental-driver-private-phases-2026-10-15.md) ·
[机器可读证据](experimental-driver-private-phases-2026-10-15.json)

原两层更高 fence 恢复在 PostgreSQL 16.15、17.11 的默认线程栈均崩溃；只调整完整
Root reader 后仍崩溃。实际崩溃栈与二进制函数序言定位到 Driver、完整 DIRECT
观察和完整 Store 历史认证叠加的栈占用。

Driver 将完整启动、迭代、屏障、节点批次拆为私有堆阶段；完整 recovery、scope、
Root checkpoint、active frame、计划、结果与 DIRECT context 保存在堆上。Root
reader 同样保留完整 admission、checkpoint。原校验、SQL 与锁序、取消优先级、
期限、共享事件 quantum、重试上限、监护 ACK 和释放行为保留，没有 metadata-only
认证或持久 proof cache。原 scoped ChildJoin 拒绝和完整 Root Continue producer 保留。

最终实测输入为 902 个文件，manifest SHA256 为
`2629e47bb772ae27ff506707cdc9fbacd7afcd0a0f016b2e63a1f7ee303e1d2b`，
Runtime Native 二进制 SHA256 为
`e254cd98ae9bac08a82d13980d6155a642866d9c616bd4bc9e0f1a6a051796d1`。
采用的六个 Rust 文件与冻结输入相同。本报告、CHANGELOG 和里程碑记录在测试后添加，
不据此获得运行资格。

同一最终二进制在两版数据库的原两层更高 fence 恢复、完整 SQL role-profile 回归
分别通过，使用默认栈和原资源上限。原七层用例在两版均以 `LeaseExpired` 失败：
PostgreSQL 16 为 1,115.07 秒，PostgreSQL 17 为 1,042.00 秒；原最终断言未到达。
这两次并发运行还与编译重叠，耗时只作实际观察，不构成容量数据。七层恢复未获资格。

格式、严格全工作区/all-targets/all-features Clippy、98 个 suite / 2,047 项测试
（六项原有 ignored）、45 项独立文档测试、严格 Rustdoc 和固定
`nightly-2026-10-07` 工作区检查通过。未配置数据库的 workspace 入口不算 Native。
独立 Standards、Spec 两轴对布局修改均无可执行发现；token 对照保留完整阶段体以及
Root SQL/literal 顺序。静态审查不能证明运行正确。

首次 Root-only 构建跨源码目录误复用了基线产物；检测到相同二进制后已拒绝该结果。
随后使用空 target 重新编译，真实复现 Root-only 栈崩溃。最终二进制在显式清理本任务
workspace 产物后重建；源码快照、原失败、崩溃报告、日志和回执均保存到编译缓存之外的
`.local-verification/2026-10-15/`。

下一步测量同一次 owning read 中重复的完整认证，仍须保留实际物理证据与写后 fresh
验证。完整 Native、schema36/scoped ChildJoin 组合、Linux、深层/串行/AgentLoop
恢复、故障和兼容矩阵、容量与发布门禁继续开放。RFC-0022、R1 未关闭，PR #161
保持 Draft；本增量不提升整体生产或 Supported 状态。
