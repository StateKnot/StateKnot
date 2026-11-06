<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性已发布 scoped ChildJoin 恢复验收记录

核对日期：2026-10-19。本文的 G1 指“已发布、尚未消费的 scoped ChildJoin 恢复”切片，不是缺口账本的 schema 条目编号。[English](experimental-scoped-child-join-resume-2026-10-19.md) · [机器证据索引](experimental-scoped-child-join-resume-2026-10-19.json) · [Draft RFC-0022](../rfcs/0022-namespaced-graph-frames.md#scoped-isolated-childrun-integration-in-development)。

已测提交 [`343b244d59a9f13987139fe98f802491a474640b`](https://github.com/StateKnot/StateKnot/commit/343b244d59a9f13987139fe98f802491a474640b) 的限定 G1 路径及其完整 [CI run 39228803615](https://github.com/StateKnot/StateKnot/actions/runs/39228803615) 已通过。严格离线分析实际退出 0，`full_CI_success_verified=true`、`full_CI_qualification=true`、`analysis_errors=[]`，14 个独立组件全部 verified。这是该准确源码的 CI 和限定业务路径证据；RFC-0022、PR #161、Supported 状态以及 R1/R6/R7 均未据此关闭。

本文件、英文摘要、机器索引及 tracking 同步是后续文档候选，尚无新的文档采用提交，不能把 `343b244` 的实测资格改标到未来提交。当前 G2 fresh scoped Join 开发及其测试不在该 CI 的源码或结果中。

## 精确源码与提交绑定

| 对象 | 记录 |
| --- | --- |
| 测试触发 head | `343b244d59a9f13987139fe98f802491a474640b` |
| 实际 PR checkout S | `4a05c35a9ef4f17b4176caf6b3afd85d94c39b7c` |
| S 的有序父提交 | `[aa11b4f44a948aaf2e2baba4c30a297dc828ce6d, 343b244d59a9f13987139fe98f802491a474640b]` |
| head 与 S 相同完整 tree | `bdbddf21d9bd0a98b0fc25171e0c5a2df7ed15a6` |
| head 的直接父提交 | `7565825aca72dc39ad436353fe8315d1075f1fcb` |
| 949 份选中源码摘要 | `1404f54f6de8bd777a9d4a7a832c79dacdc75f3b7f732c6eca303f33ff97f8c1` |
| 553 份编译输入摘要 | `4745bb73cca9553b03d94df998b7d8b21a7b442c4357bb9e3038ee117fac9820` |

S 来自真实 checkout 日志及官方 commit object，不从 head 或 ref 名猜测。19 个有 checkout 的 job 都核对到 S，两个汇总 job 没有 checkout。官方树/三提交链验证区分 C9dd 后的 3 文件 Host fixture/doc 修正、1 文件 schema29 cleanup 修正、9 文件 G1 实现/测试/文档/CI pin，累计精确 13 路径和一个新增 blob；C9dd 及其原 37 路径仍是历史对象证明。相对 main 的 131 个提交均保留准确 DCO。PR 仍为 Draft，main 未由本轮发布推进。

本机定向执行使用 `scoped-child-join-g1-green-source-v1` 的实际 Native 二进制；最终六门禁和提交只增加六项非编译差异：CHANGELOG、RFC0022 和四个 CI 文件。全部 553 个编译输入逐字节相同。机器索引分别保存这两个源快照，不能把非编译 overlay 说成重新编译过的二进制。

## 已交付的业务边界

每轮恢复在实际 claimed Run、fence、journal、Root/leaf checkpoint 和 recovery plan 上核完整 owning Join。普通 scoped child owner 必须绑定 typed、Exclusive executor、原 publication/binding、未消费状态以及全 slot 的真实 terminal、quarantine、registry/output schema 校验；读取不授予 launch 或写权限。

正向从原 F1 的真实 frame/Start/spawn/registration/publication 出发，重新连接 Store、重建 registry 并 claim 更高 F2。实际 callback 读取原 child Join 后返回 Completed；原事务把成功结果与原 publication 的消费原子提交一次。随后真正 scoped barriers、frame Return、Root finish 与相同 terminal handoff 重放成功。原 F1 attempt 保持 Executing，不补造 completion；F2 attempt 成功；child 不重启，settled subtree 只加一次。

**该 G1 正向没有杀死并替换 OS 进程。** 它验证同一测试进程内的新连接、新 registry 与更高 fence。其 child 是未执行应用/provider I/O 的真实 admission，沿既有受信 control-plane failure 终态、结算与发布建立前置条件；不是 fresh callback 自助 spawn 或真实 child 应用成功交付。父最终仍为 **Active 的 Root terminal handoff**，并未由该测试提交 Succeeded。已有独立 cold cancellation/process 回归不扩大这条 G1 正向的故障资格。

三个实际边界用例分别保留：fresh Root frame target 拒绝、drive 前已隔离的 published child 拒绝，以及正常活动 frame 的下一 unsupported target 在 F2 caller rebind 前拒绝。它们核精确错误、零应用/DIRECT/Start 或 rebind，整个 tenant 仅允许 exact-fence lease release。quarantine 保证针对 drive 前已提交隔离，不承诺 gate 后并发隔离撤销在途 I/O；最终消费仍走原原子 fresh guard。

## 实际验收

| 验收层 | 实际结果 |
| --- | --- |
| 本机六门禁 | fmt、严格 Clippy、workspace、独立 doctests、严格 Rustdoc、固定 nightly 全部实际退出 0 |
| 本机 workspace / doctests | 100 个 harness：2144 passed、0 failed、6 个原 ignored；10 个 doctest harness：48 passed |
| 本机 G1 定向 | 正向及三个边界在 PostgreSQL16/17 各执行一次，共 8 次；另六个原有回归两版各一次，共 12 次；20 次 observer/test 均实际 0 |
| 最终 CI | 21/21 job success；原 PostgreSQL 60 个必要 step 两 major 共 120 个全部 completed/success |
| CI 全量 Native | PostgreSQL16/17 各 Store267、Runtime210 全部通过，suite exit0；真实并行 owner 均退休，monitor0，default features/default stack/profile 与 553 输入吻合 |
| 证据收集 | bootstrap17 GET；terminal99 GET，54 ZIP 摘要及 1544 索引文件完整核对；analysis 实际退出0 |

本机 Native 使用原 opt1、debug0/incremental0、默认栈和保留 assertions/overflow checks 的新构建；workspace 条件数据库入口不代替 Native。完整 CI 的 Runtime210 结果包括该 G1 selector 的真实成功行，但全量 suite 未额外开启 nocapture，不据此伪称新增专用 marker step。本机定向原始 stdout/stderr 独立保留。

## 保留历史与剩余范围

旧 C9dd (`9dd396b0…`) 的 run [39221903796](https://github.com/StateKnot/StateKnot/actions/runs/39221903796) 仍是 16/21 success 的失败运行：两版 Host 的旧四-job oracle 和 PG16 schema29 cleanup 失败原文保留，PG16 Runtime 被 runner 停止，不能称旧 Runtime206 全量通过。旧 37 路径本地 2140/Runtime206 记录与 portable bundle 原样保留，既不改写为 2144/210，也不从新结果反授旧源码资格。

fresh scoped Join suspension、partial-spawn 和其进程/COMMIT 故障链、完整 scoped cancellation、真实平台 consumer/stdio owner、历史窗口、容量/部署及独立安全验收仍分别交付。G2 新测试、Natural RED 或开发候选不进入 `343b244` 的 source949/compiled553/Runtime210 账本。R1、R6、R7 退出条件和 RFC Draft 保持不变。

机器索引给出原 receipts/日志索引的 repo-relative 本地保留位置、字节数与 SHA-256；它是精简导航，不复制54个ZIP，也不是单独可重建整套源码或重跑 CI 的 portable archive。原始文件和旧37 portable bundle 均未修改。本次文档采用需要独立核对新源码差异；未来文档 commit 不继承为已测 CI head。
