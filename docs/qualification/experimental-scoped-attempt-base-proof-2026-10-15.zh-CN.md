<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 scoped attempt 完整 base 证明

[English](experimental-scoped-attempt-base-proof-2026-10-15.md) ·
[机器可读证据](experimental-scoped-attempt-base-proof-2026-10-15.json)

普通 scoped 成功 attempt 的公开 `load_node_attempt` 原本在同一次
repeatable-read 事务中完整认证同一 base 两次。现在这个公开读取消费第一次完整认证
产生的私有、不可 Clone 证明。只有框架 Start 识别返回无匹配、完整 base/历史校验
通过、实际 Start journal anchor 校验通过且 activation 非 Root，才会产生可用证明。
证明、Start 和完整实际 durable result 的 activation 必须三方精确相等。实际
completion 的 canonical bytes 与各投影、completion journal anchor、result bytes、
result owner、完整 result head 及真实 tool/model/Join bindings 仍按原规则校验。

只有这个公开读取在自身事务内传入证明；mutation、post-guard 和分页历史读取继续
传入 `None`，完整执行旧路径。Root 与已识别的框架 Start 不发行可用证明。没有引入
metadata 认证、持久缓存或跨事务复用；schema、SQL literals、锁、fence、期限、
重试和事件上限、资源限额均未改变。

最初的无探针候选虽然通过 Store 包检查和选择性 Native，但完整工作区 Clippy
在 Runtime ProviderNative Agent Future 中发现两条 `E0275` Send trait 递归错误。
本次采用的候选将完整私有 `verify_node_attempt_start_with_base` Future 放到
`stateknot_core::BoxFuture` 边界后。事务借用为 `'a`，Start 借用为 `'start: 'a`，
返回的证明仍借用原 Start；原方法全部 123 个 body token 与方法前后源码保持一致，
完整 `after_start` 阶段没有改动。没有增加 recursion limit、线程栈或资源限额。
[原始失败](evidence/scoped-attempt-base-proof-2026-10-15/workspace-clippy-before.log)、
[body 证据](evidence/scoped-attempt-base-proof-2026-10-15/source-proof.json)与
[独立审阅](evidence/scoped-attempt-base-proof-2026-10-15/independent-review.json)均保留；
针对完整 Future 类型擦除补丁的独立审阅为零项发现。
[原始补丁](evidence/scoped-attempt-base-proof-2026-10-15/erase-only.patch.gz)及含原生
EOF 空行的日志以无损 gzip 保存；回执 hash 仍核对解压后的原始字节，manifest
同时绑定实际压缩字节，没有规范化原始空白。

最终输入为 899 个文件，manifest SHA256 为
`5cc4e2cd0ab5824b87510dfaea6e95c08e3f1cf0cd20f6e8ebcad0da0b103645`。
本次采用的三个 Rust 文件与该输入一致；crate 目录及选定构建配置共 515 个文件也
逐项核对一致。采用文档在测试后新增，不继承运行时资格。fmt、严格完整工作区
all-targets/all-features/locked Clippy、98 个 suite 的 2,048 项工作区测试
（6 项既有 ignored）、45 项独立文档测试、严格 Rustdoc 和固定
`nightly-2026-10-07` 工作区检查全部通过。静态门禁清除了数据库环境变量，不能把
这些工作区数量算作 Native 验收。

显式清理包产物后真实重编译的 Store 二进制 SHA256 为
`8abd33d1dbd7077608a7a3fd16cf77d4beb3b94ecc8bf7261c312e846b2883ad`。
相同六条选择性 Native 在 PostgreSQL 16.15、17.11 各全部通过，每条均为
1 passed、0 failed、0 ignored。覆盖 scoped 成功读取及真实物理损坏拒绝、
scoped takeover、框架 caller completion 拒绝、checksum-valid Return 替换拒绝、
物理 attempt 历史与身份、真实 tool/model bindings。保持默认线程栈、原 fixture
期限及全部 owning 认证；这 12 条通过不能替代完整 Native suite。

此前独立带探针诊断在两版 PostgreSQL 各五次公开读取中观测到 SQL 从每次 93 条降到
53 条：完整 base、closure、admission、history 认证从两次变为一次，真实 pending
bindings 仍执行校验。这些数据属于另行冻结的诊断二进制，不属于上述最终二进制。
并发和缓存影响使原样本也不足以声明时延改善；不能把它们转授为最终二进制的
时延、容量或七层恢复资格。

较早的 `1fccf1816a2549d8ca7296b635434b52b30140e9`
[CI](https://github.com/StateKnot/StateKnot/actions/runs/38700608003)中，两项 PostgreSQL
job 均因原 30 分钟 job 总时限终止，结论为 cancelled。Store Native 各 251/251
通过，其他 11 项 job 成功；Runtime 与后续资格步骤没有完成。这是不同源码的未完成
CI，不能当成本次增量通过。

[证据目录](evidence/scoped-attempt-base-proof-2026-10-15/README.md)保存原始日志、冻结
源码摘要、原编译失败、body 证明、独立审阅与诊断 SQL 捕获，并提供核对 hash、
测试摘要、body 等价和采用输入的验证脚本。完整 Store/Runtime Native、Linux、
原 higher-fence/七层重新验收、最终 schema36/scoped ChildJoin 组合、完整故障与
兼容矩阵、容量和发布门禁仍开放。RFC-0022 与 R1 没有关闭，PR #161 仍为 Draft；
本增量不授予 Supported 或生产发布状态。
