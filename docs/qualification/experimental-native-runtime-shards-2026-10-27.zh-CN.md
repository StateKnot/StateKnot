<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Native Runtime CI 执行组织

更新日期：2026-10-27。本增量只改变 CI 执行组织，不改变公开 Rust API、数据库 schema 或产品支持级别。

PostgreSQL 16、17 作业继续使用同一次冷构建产生的 Store、Runtime 两份二进制。Store 执行完整 276 项测试；Runtime 的完整 229 项实际库存按名称排序后交替分配给两个独立数据库和进程，各自使用完整精确名称与单测试线程。两组必须无交集、恰好覆盖生产者的完整库存，并分别满足预期通过数和过滤数。保留六份原始输出流，不拼造一个完整 Runtime 输出。

执行格式为 `stateknot-native-runtime-shards-execution-v1`，当前契约为 `stateknot-postgres-native-shared-build-ci-contract-v2`。三个原始 leader 身份分别拥有 helper 和实际二进制；校验绑定实际进程身份、数据库路由、源码、二进制摘要、profile、ABI、完整输出与清理事实。原单流命令继续严格校验旧格式，不能用来认证新布局。生产者仍只导出两份二进制，作业、checkout 和 artifact 身份不变。

租约 30 秒、READY 90 秒、pair 1,800 秒和 Native 作业 30 分钟预算均保持不变。本次修复依据实际排程证据：[运行 39548415549](https://github.com/StateKnot/StateKnot/actions/runs/39548415549) 的双版本 Store、Runtime 测试均全部通过，但 PostgreSQL 16 的 Runtime 用时 1,771.99 秒，作业达到总时限后取消；PostgreSQL 17 Runtime 用时 1,696.22 秒。原取消作业仍保留为未成功。

本机已通过新增 60 项编解码及控制测试、原有 12 项 artifact 控制、Python 3.9 语法检查和两个 Shell 语法检查。这些是本机控制验证；新三进程布局的完整 Linux 执行和全部 CI，仍须由已发布候选的真实运行证明。R1 和发布资格继续开放。

同一交付只更新存在安全公告的 npm 传递依赖锁记录。网站和冻结 MCP runner 的审计均恢复为零漏洞，原有浏览器及冻结协议门禁已在本机通过。MCP runner 和协议要求版本保持不变，未计分能力保留原始结果。

[English](experimental-native-runtime-shards-2026-10-27.md)
