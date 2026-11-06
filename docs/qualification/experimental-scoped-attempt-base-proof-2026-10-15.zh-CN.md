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

随后，采用提交 `83559f7651f48d65755c1fc5fd8a9ba82e721193` 的相同冻结源码已在
macOS aarch64 完成原四组本机 Native；保留默认线程栈、原租约、期限和资源上限：

| Suite | PostgreSQL 16.15 | PostgreSQL 17.11 |
| --- | ---: | ---: |
| Store | 252 passed | 252 passed |
| Runtime | 185 passed | 185 passed |
| Provider integrations | 2 passed | 2 passed |
| 可恢复 Agent 示例 | 1 passed | 1 passed |

每组均为 0 failed、0 ignored。Runtime 二进制 SHA256 为
`2dc582601ae7471a87b1b85ac5c90dddd92c7665be71adfd05dd974996f585ab`；
[完整回执](evidence/scoped-attempt-base-proof-2026-10-15/full-native/receipt.json)
绑定四个二进制、构建回执、源码核对、原始日志与测试清单。两版 Runtime 均已通过
原 higher-fence、真实七层、serial siblings 和 shared quantum 的完整原断言。
这只关闭本候选的这些本机回归；此前仅布局候选的租约过期失败仍独立保留。

[覆盖清单](evidence/scoped-attempt-base-proof-2026-10-15/full-native/coverage.json)将
185 个 Runtime 入口区分为 164 个普通数据库/父场景、15 个辅助子进程 worker、
一个逻辑 restore 资源门禁、一个 PITR 资源门禁和四个 harness/协议自测。
worker 缺少父进程输入时可以正常 return；两项资源分支未设置独立开关时也可以
return，即使数据库已设 mandatory。因此每版 440 个 harness pass 不等于 440 个
独立数据库故障场景。这两次普通 Runtime 日志没有 backup/restore 或 PITR marker；
验证器另行核对原本机 process/COMMIT-loss、角色与 maintenance marker 数量及内容。

另有[独立资源证据](evidence/scoped-attempt-base-proof-2026-10-15/resources/receipt.json)：
本机两版均使用同一 Runtime 二进制和强制容器/restore 开关完成逻辑备份恢复，
各 exact 1 passed、唯一 marker 1；实际源/恢复隔离、schema、完整持久快照、跨租户
拒绝及非初始重放断言均为 true。Linux x86_64 的命名时间点恢复由
[CI 38709873198](https://github.com/StateKnot/StateKnot/actions/runs/38709873198)
独立验收，实际 PostgreSQL 为 16.14/17.11，各 exact 1 passed、唯一 marker 1；
真实 base backup、已归档 WAL、排除时间点后写入及全部重放断言通过。
CI 实际 checkout 为合成 merge commit `4c873c3a8615cae8372daff23f36c5976d5e5883`，
完整 tree `781d3d550c5b45554dada9723707508d93dc8703` 与采用 head 的完整 tree
精确一致，Cargo.lock 也相同。最初仅比较 commit ID 的拒绝记录与后续更严格的
[完整 tree/锁验证](evidence/scoped-attempt-base-proof-2026-10-15/resources/pitr-verification.json)
同时保留。这些资源结果不转授其他 CI 步骤或 Linux 深层 Driver 恢复资格，
没有据此声称完整 CI 通过。

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

采用 head 本身的 [CI 38709873198](https://github.com/StateKnot/StateKnot/actions/runs/38709873198)
也未完整通过：11 项 job 成功（含两项独立 PITR），两项普通 PostgreSQL job cancelled。
两项均有明确 annotation，原 30 分钟 job 总时限已耗尽。Store 各完成
252 passed / 0 failed / 0 ignored，harness 分别耗时 1,231.22/1,243.15 秒。
Runtime 均已进入 185-test harness，但没有终态摘要，日志也未出现显式 failed-test 行；
既不能算 Runtime 通过，也不能据此断言代码断言失败。后续 provider/example 与专用资格
步骤 skipped，缺少 artifact 的 annotation 也原样保留。
[最终快照、annotations 与完整 job 日志](evidence/scoped-attempt-base-proof-2026-10-15/ci-83559/observed.json)
与本机成功证据独立保存；原 job/harness 耗时不构成吞吐或容量结论。

[证据目录](evidence/scoped-attempt-base-proof-2026-10-15/README.md)保存原始日志、冻结
源码摘要、原编译失败、body 证明、独立审阅与诊断 SQL 捕获，并提供核对 hash、
压缩及原始 hash、测试摘要、body 等价、采用输入、Runtime 覆盖与独立资源 marker
的验证脚本。Linux 完整深层/serial/AgentLoop 恢复、全部专用 CI 资格、最终
schema36/scoped ChildJoin 组合、完整组合故障与兼容矩阵、容量和发布门禁仍开放。
RFC-0022 与 R1 没有关闭，PR #161 仍为 Draft；
本增量不授予 Supported 或生产发布状态。
