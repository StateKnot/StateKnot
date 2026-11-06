<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 claimed 帧恢复验收

本报告只验证提交 `8285f25130eeba18b7f6f2059089f851a64e1fac`、tree
`9428ca4c0c7e48b58c85beeeaa120323739f9d42` 的限定 Core/PostgreSQL 增量。
实际嵌套 Driver 和完整应用历史 replay 尚未完成。RFC-0022 保持 Draft，
R1/G3/R6/R7 保持开放。[机器报告](experimental-claimed-frame-recovery-2026-10-10.json)
保存精确源码身份、实际结果、原始限制、日志摘要和失败记录。

claimed scoped planning 校验原 claimed 上下文、全活动帧快照、精确 leaf、
有界 pending-result 页面及完整物理尝试历史。返回前重新校验 journal、Root/leaf
和数据库时钟下的原 fence；规划观察不授予 dispatch 权限。精确 checkpoint 的
result namespace 矛盾在重读 cursor 行之前拒绝，损坏隔离仍绑定原有效 owner。

私有 return intent 在堆上解码；规范字节、摘要版本、公共 return 记录、schema
和依赖锁文件均未改变。

## 精确源码通过结果

- 完整 workspace/all-targets/all-features/locked 严格 Clippy 和 Rustdoc 通过。
  工作区 98 个 suite、1,909 项通过、三个显式 ignore；下面独立的必跑原生门禁
  实际执行 PostgreSQL，不能把未配置数据库时的工作区结果当作数据库证明。
- PostgreSQL 16.15/17.11 完整 Store 各通过 10 项 unit、一项 artifact、182 项 native；
  native 分别耗时 320.94/355.44 秒。完整 Runtime 各 140 项通过，包含 Schema 33
  下真实独立 LOGIN 连接的 trusted-role profile。
- Linux 完整七层 return 回归在 74.87 秒内通过，固定 Rust 1.88、原 2 CPU/2 GiB
  限制和默认线程栈。847 个源码文件、文件模式和锁文件与冻结源码完全一致。
  未放宽栈、lease、deadline 或断言限制。
- 本机和不可变 CI ASan 均重放 2,297 个相同种子，三个目标各运行 10,000 次实际变异，
  全部在固定限制内。峰值 RSS 本机为 512/1,019/448 MiB，CI 为 428/1,039/299 MiB。
  源码快照、manifest、工具、锁文件、实际统计和日志摘要已独立核对。
- [CI 38055431022](https://github.com/StateKnot/StateKnot/actions/runs/38055431022) 的全部
  13 项和 [Supply chain 38056473049](https://github.com/StateKnot/StateKnot/actions/runs/38056473049)
  通过。merge `721b38b9eff13495f5b24283d26af2c342f6f9d9` 的 tree 与冻结源码相同，
  双亲为 `aa11b4f`/`8285f25`；ASan artifact 为 `11671561451`。
- 当前精确源码的公共 reader 在 PostgreSQL 16/17 只读逻辑恢复中校验七个真实旧 return
  （depth 1–7），保留规范字节/摘要、完整祖先/caller 证明、16 个 closure/40 个 caller/
  四个 provider binding、52 表行数及行摘要、精确 catalog 和 33 条迁移记录。
  这是当前 reader 的逻辑备份兼容；不证明 ACL 恢复、已发布 N-1/N-2 历史程序，
  也不将旧 reader 的 immutable mutation retry 验收继承给本增量。

## 保留记录与开放工作

Runtime 16 首次全量有 139 项通过，一项在新连接上报 `Protocol` unknown message
`\0`，发生在该测试业务执行前。TLS 已明确 Disabled，原始 wire 原因尚未证明。
独立复现入口通过后，相同源码/配置/限制的完整 140 项在 359.28 秒内通过；原测试
和服务日志分别保留。父提交 f05 的 SSLRequest 失败是另一条独立记录。

Linux 首次事后 guard 因 Docker mount 枚举顺序改变而失败，原失败保留。完整字段
排列证明解释两个完整配置摘要；没有忽略字段或 mount，没有重跑测试；修正报告
明确关联原 guard 失败与证明。

固定 nightly ASan 通过，但 Runtime 深层泛型 Future 的 `Send` 推导触发未来编译器
递归限制警告。另一个增量正在实现真实私有 BoxFuture 边界；未修改 recursion limit，
本报告不向后续增量转移验收。

scoped retry wakeup、多图完整应用 replay、Root Wait/Terminal 的持久完整 disposition、
实际嵌套 Driver 与 scoped child Join 继续开发。旧 Root 历史缺少完整 disposition 时须
返回操作性错误，不能产生完整 replay 报告或错误隔离。完整生产故障、历史程序兼容、
容量、独立安全及发布门禁仍开放。父 f05 的资格保留在独立的
[全帧报告](experimental-whole-frames-2026-10-10.zh-CN.md)中。
