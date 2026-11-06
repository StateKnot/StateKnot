<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性嵌套 Driver 组合验收：2026-10-11

[English](experimental-nested-driver-combination-2026-10-11.md) ·
[机器可读证据](experimental-nested-driver-combination-2026-10-11.json)

这些结果只绑定提交 `f9cae2db2efc42bb7a399b2428418a3a59858304`、树
`c239c48fe2c22f331aba629789f249ee63c0b6f3` 及全部 880 个跟踪文件。
R1 仍开放，RFC-0022 仍为 Draft；完整原生 CI 仍失败，后续源码需要独立验收。

| 实际范围 | 结果 |
| --- | --- |
| Rust 1.88 工作区静态门禁 | 格式、严格 all-targets/all-features locked Clippy、98 个 suite 的 2,020 项通过（六项 ignored）、45 项独立文档测试及严格 Rustdoc 通过。未配置数据库而提前返回的入口不构成 Native 证据。 |
| 固定 `nightly-2026-10-07` | 同一源码完整工作区/all-targets/all-features 严格检查通过；早先 `01beba98` 的 Driver future 失败仍保留。 |
| 本机固定 ASan | 314 个 reader、71 类 enum / 298 个 variant、2,304 份种子，包含原终态输入的七份新种子。三个目标均重放全部种子并各完成 10,000 次实际变异；峰值 RSS 498 / 820 / 470 MiB。 |
| CI 固定 ASan | 实际 checkout 为合成合并提交 `ff5584c201799660ed3fb11e81291eff9b50c4ac`，其父提交、源码树和完整文件摘要与冻结 f9 相等。下载报告和原始目标日志摘要已核对；三个目标各完成 10,000 次实际变异，峰值 RSS 456 / 1,026 / 271 MiB。 |
| 完整 trusted SQL role 原测试 | PostgreSQL 16.15、17.11 各一项完整原测试实际通过，schema 34，默认栈。覆盖独立非超级用户 LOGIN、ACL/漂移拒绝、child Join、提交/provider 恢复、24 路竞态、scoped Start/Barrier/Rebind/Return/Wait/Closure、独立 retention、非空重应用和精确 catalog。原默认栈溢出在这个组合已解决。 |
| 完整 CI run 38079695038 | 十一项 job 通过；必须执行的 PostgreSQL 16/17 Store 检查各 240 项通过、十项失败，Runtime Native 和后续步骤未到达。 |

原生失败仍明确保留：七项首次 INITIAL Root 的来源 parent 校验、两项故意破坏
checkpoint canonical bytes 后调用完整公开 Run 读取，以及七层完整应用重放超出原租约。
正向来源 Root 支持、正确的损坏断言和完整拥有记录重放的成本边界分别继续处理；本报告
没有认证 metadata 捷径或增加租约。

本机第一次 ASan 在开始 fuzzing 前失败，原因是 PATH 缺少已固定的
`cargo-fuzz 0.13.2` 目录。原日志与前置报告保留；替代运行只修正 PATH，对相同源码
执行原完整门禁。保留 131,072 字节输入、10,000 次变异、每目标 60 秒、每输入十秒、
2,048 MiB RSS、两个编译 job、AddressSanitizer 和 `cfg_fuzzing=false`。
两个锁文件及全部源码运行前后摘要一致。

本机 checkout 只含跟踪源码，静态与 fuzz 缓存首次为空；报告记录实际编译来源、角色
二进制摘要。普通检查使用默认线程栈和
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`。
编译前核对磁盘，未提高栈、租约、截止时间或超时。日志和回执保留在缓存外；机器可读
报告记录全部保留路径、命令、退出码和摘要。

这些结果不转授 schema 35、取消监护修复、scoped Driver ChildJoin、过期绝对 Wait、
完整 Native、真实历史 N-1/N-2、R1 或 R6/R7，也不提升实验 API 到 Supported 或
RFC-0022 到 Accepted。
