<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 whole-frame Store 限定验收

本记录只验收冻结 Core/PostgreSQL 增量至 whole closure，不关闭 R1，也不证明实际
nested Driver 执行。[PR #161](https://github.com/StateKnot/StateKnot/pull/161) 与
RFC-0022 仍为 Draft。[机器证据](experimental-whole-frames-2026-10-10.json)保留来源、
原始失败、日志摘要和限定范围。

源码为 `f05d473fcef967bbe1690768562648b8e3cefe3e`，tree 为
`0b2449e15c4bde35b9fd26a0876ac6a06cc37661`。
[CI 38016667371](https://github.com/StateKnot/StateKnot/actions/runs/38016667371) 的全部
13 项任务及[Supply chain 38016932475](https://github.com/StateKnot/StateKnot/actions/runs/38016932475)
通过；CI merge `0662d16f8e45f7479d39464a9e8e59dc79af7379` 保持相同 tree。

| 本地同源码验证 | 结果 |
| --- | --- |
| 工作区 | 1,900 passed、零失败、三个明确 ignore |
| 严格 Clippy、Rustdoc、format | 通过 |
| PostgreSQL 16.15 / 17.11 Store | 各 10 个单元、1 个 Artifact、173 个 native 通过 |
| PostgreSQL 16.15 / 17.11 Runtime | 各 140 通过，包含 Schema 33 独立 LOGIN 角色组合 |
| ASan/libFuzzer | 本地和不可变 CI artifact 11655799106 各重放 2,297 种子，三个目标各完成 10,000 次实际变异，保留原限额 |
| 当前 reader 的业务恢复 | 两版均保留 52 表计数/摘要、精确 catalog/checksum、16 个 closure、40 个 caller、4 个 provider binding |

未修改断言的七层 return 回归在固定 Rust 1.88 Linux 普通测试栈上通过。普通与 closure
completion 两个 witness 都保留完整认证；私有 completion restoration Future 不再扩大
祖先重放布局，修正了此前只隔离 caller anchor 仍然溢出的不完整修复。

早期本地全量 Store 各有 172 通过、1 个 `LeaseExpired`；随后相同源码、原限额的串行
全量验证通过。Runtime 16 首次有 139 通过、1 个新连接 SSLRequest `0x00` 握手失败，
发生在查询之前；相同源码与配置的完整重跑通过。原始失败均单独保留，不改记为成功。

保留业务 corpus 由 `bc3a3f1` 写入；当前 f05 公开 reader 对新的逻辑备份恢复逐项核对，
包括原始 Root/lifecycle 与不可变 Existing retry。这不验收备份 ACL 或历史 N-1/N-2
可执行程序；此前 schema-only 证据仍单独绑定原来源。

claimed scoped 规划属于另一个开发增量，不继承本文的源码验收。实际 nested registry/Driver
派发、scoped 应用重放、child/Join 执行、完整进程故障、历史兼容与生产容量继续开放。
本记录不声明 Supported，不关闭 R1/G3，也不产生发布或部署。
