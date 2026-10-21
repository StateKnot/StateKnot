<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 请求事实与 PITR 清理组成后的原配置本地门禁

2026-10-15。实际main工作树组成已采用A2A请求事实与PITR test-only清理确认后，
通过全部六项原**opt0**本地门禁：workspace98个suite、2073 passed、0 failed、6个原
ignored；独立doctest45 passed、0 failed/ignored。fmt、all-target/all-feature严格
Clippy、严格Rustdoc及固定nightly2026-10-07也均exit0。65.43秒是门禁耗时，不是性能
数据。[English](experimental-composed-request-facts-pitr-2026-10-15.md)。

执行时HEAD为 `04608c6b20a7c18d6b042eeccc83dd38d096b1d1`，工作树已放入PITR helper
及127个交付路径。**531个编译输入的路径/字节/hash及当时全部2182个非ignored文件
在运行前后逐字相同**；随后新增的报告/tracking文本不冒称属于该测试快照。默认栈、
原资源/lease预算、debug assertions和overflow checks保留；incremental/debug-symbol
缓存控制与opt level分别记录。DB测试环境已清除，条件数据库入口不算Native执行。

[机器记录](experimental-composed-request-facts-pitr-2026-10-15.json)和
[证据封签](evidence/experimental-composed-request-facts-pitr-2026-10-15/seal.json)
绑定每份回执/raw log、命令环境、Git状态与完整前后清单。最初runner仅因list排序辅助
断言停止，尚未启动产品命令；当时531项路径/字节/SHA map已相等。原runner及错误
保留，修正仅比较map，不改产品源码、资源或校验断言。

[A2A记录](experimental-a2a-request-facts-2026-10-15.zh-CN.md)和
[PITR记录](experimental-pitr-cleanup-confirmation-2026-10-15.zh-CN.md)保留各自原始
source/binary/执行范围与封签；PITR最初顶层报告中的组成“待执行”是历史状态，本次
后续记录才提供实际本地门禁结果。旧PG16 WAL仍保留，全部失败路径清理仍开放；
旧916次Native及定向PITR/A2A执行都不转授为组成版本的新完整Native资格。

默认校验完全离线；`--workspace`另从已发布A2A923归档和精确PITR单文件patch重建
531编译输入，并对照实际main路径/字节。本小包不新增完整源码归档、binary或cache。
本轮没有执行完整Runtime189/四套Native、完整CI或独立TCK/CLI/release profile，
不授予容量、时延、安全或完整生产/R1资格。RFC-0022/RFC-0025仍为Draft。

```console
python3 docs/qualification/evidence/experimental-composed-request-facts-pitr-2026-10-15/verify.py --workspace
```

随后采用提交 `ae19684cc029b4309a3fdfb21f5150ebedb36451` 保持相同531编译输入字节；原执行Git context仍保留04608及其当时未提交差异，不回填历史。
