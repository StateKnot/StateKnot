<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 实验性 PostgreSQL 测试 profile：限定执行记录

2026-10-15。命令局部 test opt1 配置的六项严格门禁和十个 Native suite 命令全部 exit0；但 **runner 最终 exit1**：收尾发现两个新缓存根均缺 `CACHEDIR.TAG`。原失败回执保留，没有生成原成功 summary，也没有补标记、清理或重跑。这是产品命令的限定执行记录，另有通过的只读资源审计，不把整轮资格标绿。[English](experimental-postgres-test-profile-2026-10-15.md)。

[机器记录](experimental-postgres-test-profile-2026-10-15.json)、[原始产物索引](evidence/experimental-postgres-test-profile-2026-10-15/artifact-index.json)与[离线校验器](evidence/experimental-postgres-test-profile-2026-10-15/verify.py)保留精确边界。

## 源码与构建

冻结923文件 tree 为 `3a84c68eb9dd9df1804a7e24801516e11c9ee01ca0c6f243a4fdfaf76cbe124c`。通过已公开的[A2A923源码归档](evidence/experimental-a2a-request-facts-2026-10-15/source/source.tar.gz)加[单个 PITR 测试文件差异](evidence/experimental-postgres-test-profile-2026-10-15/source/reconstruction.json)即可重建；本包不新增完整源码归档或二进制。实验没有修改生产代码、SQL 或 Cargo 源码 profile。

全部531个保守 crate/config/fixture 供应输入与组合 main 在执行前及最终核对时一致，规范输入摘要为 `ddaf1aba9089af825b870d3c1466969d8967d59ee8805564b302b4c16eebc4b8`。这不是 compiler-read 闭包或 whole-main-Git-tree 相同声明。此前[原 opt0 记录](experimental-composed-request-facts-pitr-2026-10-15.zh-CN.md)执行的是实际 main 的2182文件，六门禁原回执只通过531个精确输入桥接；没有将其转授为这个完整923源码或组合完整 Native 的匹配 opt0 基线。

命令环境为job局部构建 incremental0、dev/test debug0、**test opt-level1、debug-assertions=true、overflow-checks=true**，保留严格 Rust/Rustdoc warnings、Rust1.88.0 与 nightly2026-10-07 检查。默认栈、租约、deadline、quantum、fixture资源、SQL和完整认证不变。原six-gates summary中dev/release的简写仅指源码配置不变，不表示dev debug环境未变。build script/proc macro 保留原 build-override，不把所有 artifact 都称 opt1。真实 fresh Cargo JSON、普通库 profile、五个 binary SHA 与完整库存均保存在缓存外。

构建采用 **workspace all-targets/all-features 依赖 feature union**。package 的 features 为空不代表它与原 CI per-package 默认依赖图相同。

## 实际执行

六项严格门禁全部 exit0：工作区2073 passed/6个原 ignored，独立 doctest45 passed，以及 fmt、严格完整工作区 Clippy、Rustdoc、固定 nightly。静态命令中的条件数据库入口不算 Native 证据。

| 完整 Native suite | PG16.15 | PG17.11 |
| --- | --- | --- |
| Store264 | 446.94s | 454.46s |
| Runtime189 | 719.32s | 687.62s |
| provider2 | 1.22s | 1.19s |
| DeepSeek example1 | 1.50s | 1.51s |
| 额外 `mcp_durable` facts6 | 1.67s | 1.64s |

每一项均0 failed/ignored/filtered。Runtime189含原条件worker/资源入口与harness自测，总数不表示189个独立故障profile；只有实际启用场景及明确marker才授予其单列覆盖。原四套与额外 facts 分开记录；两版原默认栈七层、serial sibling、shared quantum 均通过。原15类 Runtime marker 次数完整吻合；facts 各有 MCP2+A2A2 拒绝/丢响应 marker，保留完整 durable Start、原 input、策略/发送与 ledger 断言。

这些本机串行耗时不授予加速百分比、容量、CPU占比或CI预算结论。本轮未启用条件逻辑备份/PITR profile。五个保存程序、全部记录 PID 与两缓存均空闲，锁可获取；两TAG缺失所以缓存保留，不授清理资格。审计分别保存实际df与逻辑目录大小。只读审计没有操作数据库或卷生命周期，新任务数据库与旧卷均保留。runner收尾guard红仍是失败。

## 原CI与拟议后续配置

[CI38741332898](https://github.com/StateKnot/StateKnot/actions/runs/38741332898)的 head686c427 终态为11项成功、两项普通 PostgreSQL job cancelled；两份原 annotation 都明确原30分钟时限耗尽。Store各264通过（1323.88/825.27s），Runtime均进入189项却无终态摘要，也没有显式FAILED行；provider/example及后续资格步骤skipped。这是未完成CI，既不算断言失败，也不算完整通过。

实际 PR merge checkout 为 `b552ef62bcb9540fc4b0291b847665885201be5b`，其完整Git tree与head686c427同为 `222152eb603e960cadbb5001a80294b9de96bc84`。独立原配置 Linux x86_64 PITR job 在 PostgreSQL16.14/17.11 各 exact1/188 filtered，通过且cleanup marker为true；原两个artifact zip和源码绑定保留。它们只授予该窄profile，不授予完整Runtime或本机opt1 PITR。

9行job env候选v2（相对v1仅修正过窄的第二条注释）仅给 PostgreSQL matrix job 设置同样的局部test opt1/checks/debug/incremental值。原53steps/31run、默认features命令、镜像、fixture资源及30分钟预算未改，其他job及Cargo dev/release/发布配置未改。执行事实与v2仅注释增量另有独立复核，0严重/0剩余发现；该复核不授予仍在整理的本公开包资格。此记录冻结时它仍是拟议配置。**原完整CI job在真实per-package默认构建下的终态仍待执行**，必须在原预算内自行证明；本机union结果不转授。RFC-0022/R1、Supported与生产资格继续开放。

先运行离线校验。采用审阅的workflow与六份候选文档后，`--workspace`再核对实际完整531路径/字节/SHA以及采用的docs/workflow；采用前或后续编译输入变化时应明确拒绝：

```console
python3 docs/qualification/evidence/experimental-postgres-test-profile-2026-10-15/verify.py
python3 docs/qualification/evidence/experimental-postgres-test-profile-2026-10-15/verify.py --workspace
```

证据整理器最初两次因源码/编译清单行顺序断言失败的回执保存在tooling历史中；修正只处理证据排序，没有改变源码或重跑产品命令。

保留原空白的文本使用deterministic gzip保存，原始大小/SHA可复核。校验器不执行构建、网络、数据库或清理。
