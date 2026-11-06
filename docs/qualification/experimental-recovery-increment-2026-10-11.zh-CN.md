<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性恢复增量：2026-10-11

[English](experimental-recovery-increment-2026-10-11.md) ·
[机器可读证据](experimental-recovery-increment-2026-10-11.json)

以下结果分别绑定各自冻结源码。R1 继续开放，RFC-0022 仍为 Draft；它们不转授
后续组合树，也不构成生产发行资格。JSON 保存精确 commit、源码清单摘要、命令、
退出码、日志摘要、数据库镜像和实际编译路径；原始证据保留在其中列出的任务路径。

| 源码 | 实际范围 | 结果 |
| --- | --- | --- |
| `3236208f76b2e1146952507e3ebc92ebe5294dbb` | 完整工作区静态门禁 | fmt、严格 Clippy/Rustdoc、1,995 项测试及 45 项文档测试通过，保留三项原有 ignored。未配置数据库的提前返回入口不算原生验收。 |
| `a21534cf3a18d660c227c5daea4c83d46aff9783` | scoped 子归属、Join、取消与原祖先上限 | PostgreSQL 16.15/17.11 各实际通过 14 项测试。 |
| `a1bc43c9f2d0d2d03c954f716cc1c35fa69ab641` | MCP reconciliation 完整 suite，包含真实 HTTP 成功/错误恢复 | PostgreSQL 16.15/17.11 各通过十项测试，包含授权、并发回执与 HTTP 确认丢失。 |
| `01beba98b8e4e0ed6f52238c80abc34dfc57611f` | 完整应用重放与确定性的 Wait transcript | 每版 PostgreSQL 各七项实际探针通过；各三项只读探针对全部 53 张业务表及迁移表的计数与字节摘要保持一致。 |
| 同一 `01beba98` 源码 | 完整工作区静态门禁 | fmt、严格 Clippy/Rustdoc、1,996 项测试及 45 项文档测试通过，六项语料/其他测试 ignored。固定 `nightly-2026-10-07` 在原 Driver future 链失败，原日志保留。 |
| `bb8a60675049c52619ce96e8c1b8aa3a3fe2f353` | Root Continue 让出、Root Wait 解决后的新 Driver | PostgreSQL 16.15/17.11 各两项测试通过。更高 fence 重放完整原 disposition，每个节点只执行一次；原 Wait ACK 恢复也通过。 |

应用重放保留完整 owning record 认证，prefix proof 只作增量证明；本报告不声明
历史成本已变为线性。无法证明的旧 Wait、Terminal 或改变通用投影的历史仍返回
操作性不可用，不隔离健康数据。UTC、ISO DateStyle 与 hex `bytea_output` 使完整
Wait transcript 不受连接渲染设置影响。

首次 Root 恢复对照提前归档两份不同源码后共用一个构建目录，Cargo 为候选运行
复用了原红例二进制。该候选结论已作废，原日志保留。替代验证使用初始为空的
任务缓存，实际编译精确候选源码，逐文件核对前后摘要，并证明 DCO 提交与受测
清单完全一致。其他列出的首次构建日志也独立核对了实际源码路径；跨数据库版本
复用二进制只适用于同一份已核对源码。

普通验收保留默认线程栈，设置
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`，
未扩大租约、截止时间、超时或递归上限。构建前核对磁盘，日志和回执保存在缓存外；
没有删除源码、数据库、Git 历史或未确认缓存。

下一轮须覆盖最终组合源码、实际 Driver 的 scoped 子 Join、普通 Root 兼容、
过期绝对 Wait、逐写入进程/COMMIT/ACK 故障、固定 nightly/Linux、真实历史
N-1/N-2 程序、CI 与 R6/R7 参考部署要求。
